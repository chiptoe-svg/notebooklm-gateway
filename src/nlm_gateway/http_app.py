"""ASGI front door: bearer auth, MCP dispatch, upload/download routes, public signed links.

Every route is under /notebooklm/. Only /notebooklm/healthz and /notebooklm/dl/* skip the
bearer; Caddy exposes only /notebooklm/dl/* publicly.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import tempfile
import weakref
from pathlib import Path

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from notebooklm import NotFoundError
from starlette.applications import Starlette
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Route

from . import tools
from .errors import GatewayError, TooManyUploads, UploadTooLarge, classify
from .fetch import fetch_to_temp, mime_for
from .links import download_filename, verify_source_ref, verify_token
from .mcp_server import build_server
from .uploads import receive_upload

PREFIX = "/notebooklm"
GOOGLE_DOWNLOADS_MAX = 2  # concurrent artifact fetches from Google, across all download routes


def create_app(gw):
    google_slots = asyncio.Semaphore(GOOGLE_DOWNLOADS_MAX)
    signed_slots: weakref.WeakValueDictionary[str, asyncio.Semaphore] = weakref.WeakValueDictionary()  # one per job, while in use
    manager = StreamableHTTPSessionManager(app=build_server(gw), json_response=True, stateless=True,
                                           max_request_body_size=gw.cfg.mcp_body_max)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        # stop only what started; always run on shutdown or on a failed startup
        async with contextlib.AsyncExitStack() as stack:
            await gw.nlm.start()
            stack.push_async_callback(gw.nlm.stop)
            await gw.runner.start()
            stack.push_async_callback(gw.runner.stop)
            await stack.enter_async_context(manager.run())
            yield

    async def healthz(request):
        return JSONResponse({"ok": True, "auth": "ok" if gw.nlm.auth_ok else "auth_failed",
                             "queued": gw.store.count_status("queued"), "running": gw.store.count_status("running")})

    async def put_upload(request):
        try:
            up = await receive_upload(request, gw.cfg, gw.store, request.state.consumer, gw.now())
        except UploadTooLarge as e:
            return JSONResponse(e.to_dict(), status_code=413)
        except TooManyUploads as e:
            return JSONResponse(e.to_dict(), status_code=429)
        except GatewayError as e:
            return JSONResponse(e.to_dict(), status_code=400)
        gw.audit.write("upload", consumer=up.consumer_id, upload_id=up.id, bytes=up.size_bytes)
        return JSONResponse({"upload_id": up.id, "expires_at": int(up.expires_at)})

    class _UnlinkAfter:
        """Run the response, then always delete the temp file (Starlette's BackgroundTask is skipped on 416/400/errors)."""
        def __init__(self, resp, path: Path):
            self.resp, self.path = resp, path

        async def __call__(self, scope, receive, send):
            try:
                await self.resp(scope, receive, send)
            finally:
                self.path.unlink(missing_ok=True)

    async def stream(request, job, event: str, one_per_job: bool = False):
        if request.method == "HEAD":  # answer from the job row: no Google fetch, no audit event
            headers = {"content-disposition": f'attachment; filename="{download_filename(job)}"', "accept-ranges": "bytes"}
            if job.size_bytes is not None:
                headers["content-length"] = str(job.size_bytes)
            return Response(status_code=200, media_type=mime_for(job), headers=headers)
        try:
            async with contextlib.AsyncExitStack() as stack:
                if one_per_job:  # public link: repeated GETs must not multiply Google fetches
                    slot = signed_slots.get(job.id)
                    if slot is None:
                        slot = signed_slots[job.id] = asyncio.Semaphore(1)
                    await stack.enter_async_context(slot)
                await stack.enter_async_context(google_slots)
                path = await fetch_to_temp(gw.nlm.require(), job, gw.cfg.state_dir / "tmp")
        except Exception as e:
            err = classify(e)
            return JSONResponse(err.to_dict(), status_code=503 if err.code == "auth_expired" else 502)
        try:  # from here on the temp file is ours to delete, whatever fails
            gw.audit.write(event, consumer=job.consumer_id, job_id=job.id, bytes=path.stat().st_size)
            return _UnlinkAfter(FileResponse(path, media_type=mime_for(job), filename=download_filename(job)), path)
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    async def download(request):
        job = gw.store.get_job(request.path_params["job_id"], request.state.consumer.id)
        if job is None or job.status != "done":
            return JSONResponse({"code": "not_found", "message": "no such finished job"}, status_code=404)
        return await stream(request, job, "download")

    _STATUS = {"not_found": 404, "bad_request": 400, "auth_expired": 503, "rate_limited": 429, "quota_exceeded": 429}
    _NOT_FOUND = {"code": "not_found", "message": "not found"}

    async def source_text(request):
        """Full text of one source named by an opaque, consumer-bound, expiring ref (scope notes).

        Fetched on demand into a temp file that is always deleted. HEAD is refused: it must not fetch or audit."""
        if request.method != "GET":
            return Response(status_code=405, headers={"allow": "GET"})
        consumer = request.state.consumer
        ids = verify_source_ref(gw.link_key, request.path_params["ref"], consumer.id, gw.now())
        if "notes" not in consumer.scopes or ids is None:
            return JSONResponse(_NOT_FOUND, status_code=404)
        nb_id, src_id = ids
        try:
            async with google_slots:
                full = await asyncio.wait_for(gw.nlm.require().sources.get_fulltext(nb_id, src_id, output_format="text"),
                                              timeout=tools.READ_TIMEOUT_S)
            text = full.content or ""
        except TimeoutError:
            err = GatewayError("generation_timeout", "NotebookLM did not answer in time; try again later.")
            return JSONResponse(err.to_dict(), status_code=504)
        except NotFoundError:  # notebook or source deleted since the ref was minted
            return JSONResponse(_NOT_FOUND, status_code=404)
        except Exception as e:
            err = classify(e)
            return JSONResponse(err.to_dict(), status_code=_STATUS.get(err.code, 502))
        tmp_dir = gw.cfg.state_dir / "tmp"
        tmp_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(suffix=".txt", dir=tmp_dir)
        path = Path(name)
        try:  # from here on the temp file is ours to delete, whatever fails
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            gw.audit.write("source_text_download", consumer=consumer.id, notebook_id=nb_id, chars=len(text))
            return _UnlinkAfter(FileResponse(path, media_type="text/plain; charset=utf-8", filename="source.txt"), path)
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    def owner_if_live(job_id: str):
        job = gw.store.get_job(job_id)
        if job is None or job.status != "done" or gw.registry.get(job.consumer_id) is None:
            return None
        return job.consumer_id

    async def signed(request):
        filename = request.path_params["filename"]
        job_id = verify_token(gw.link_key, request.path_params["token"], filename, owner_if_live, gw.now())
        if job_id is None:
            return Response(status_code=404)
        job = gw.store.get_job(job_id)
        if job is None:
            return Response(status_code=404)
        return await stream(request, job, "signed_download", one_per_job=True)

    starlette = Starlette(lifespan=lifespan, routes=[
        Route(f"{PREFIX}/healthz", healthz),
        Route(f"{PREFIX}/uploads", put_upload, methods=["PUT"]),
        Route(f"{PREFIX}/jobs/{{job_id}}/download", download),
        Route(f"{PREFIX}/sources/text/{{ref}}", source_text, methods=["GET", "HEAD"]),
        Route(f"{PREFIX}/dl/{{token}}/{{filename}}", signed, methods=["GET", "HEAD"]),
    ])

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return await starlette(scope, receive, send)
        path = scope["path"]
        if path == f"{PREFIX}/healthz" or path.startswith(f"{PREFIX}/dl/"):
            return await starlette(scope, receive, send)
        headers = dict(scope.get("headers") or [])
        authz = headers.get(b"authorization", b"").decode("latin-1")
        token = authz[7:].strip() if authz.lower().startswith("bearer ") else None
        consumer = gw.registry.authenticate(token)
        if consumer is None:
            return await JSONResponse({"code": "unauthorized", "message": "missing or invalid bearer token"}, status_code=401)(scope, receive, send)
        scope.setdefault("state", {})["consumer"] = consumer
        if path == f"{PREFIX}/mcp":
            length = headers.get(b"content-length", b"0").decode()
            if length.isdigit() and int(length) > gw.cfg.mcp_body_max:
                return await JSONResponse({"code": "bad_request", "message": "request body too large"}, status_code=413)(scope, receive, send)
            return await manager.handle_request(scope, receive, send)
        return await starlette(scope, receive, send)

    return app
