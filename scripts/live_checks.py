"""One-off live checks against the operator's account (spec §11.3).

Uses 2 Audio Overviews + 1 report. Creates a scratch notebook and deletes it at the end
unless --keep. Prints facts only; the operator judges check 2 and check 5 by eye.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import tempfile
import time

from notebooklm import AudioFormat, AudioLength, NotebookLMClient, ReportFormat
from notebooklm.options import ClientConfig, WebBackendConfig, WebSessionOptions

URL_A = "https://en.wikipedia.org/wiki/Podcast"
URL_B = "https://en.wikipedia.org/wiki/Lighthouse"


async def main(keep: bool) -> None:
    config = ClientConfig(backend=WebBackendConfig(session=WebSessionOptions(keepalive_interval=600)))
    async with NotebookLMClient.from_storage(config=config) as client:
        nb = await client.notebooks.create(f"gw-livecheck {int(time.time())}")
        print("notebook:", nb.id)
        try:
            a = await client.sources.add_url(nb.id, URL_A, wait=True, wait_timeout=300)
            b = await client.sources.add_url(nb.id, URL_B, wait=True, wait_timeout=300)
            sizes = {}
            for label, src in (("A", a), ("B", b)):
                st = await client.artifacts.generate_audio(
                    nb.id, source_ids=[src.id], instructions=None,
                    audio_format=AudioFormat.BRIEF, audio_length=AudioLength.SHORT, language="en")
                print(f"audio {label} accepted:", st.task_id, st.status)
                done = await client.artifacts.wait_for_completion(nb.id, st.task_id, timeout=1800)
                print(f"audio {label} final:", done.status, done.error)
                with tempfile.TemporaryDirectory() as d:
                    p = os.path.join(d, "a.m4a")
                    await client.artifacts.download_audio(nb.id, p, st.task_id)
                    sizes[label] = os.path.getsize(p)
            audios = await client.artifacts.list_audio(nb.id)
            print("CHECK 1 audio artifacts in one notebook:", len(audios), "(need 2)")
            print("CHECK 4 short M4A sizes (bytes):", sizes)

            st = await client.artifacts.generate_report(
                nb.id, report_format=ReportFormat.BRIEFING_DOC, source_ids=[a.id], language="en", extra_instructions=None)
            await client.artifacts.wait_for_completion(nb.id, st.task_id, timeout=1800)
            with tempfile.TemporaryDirectory() as d:
                p = os.path.join(d, "r.md")
                await client.artifacts.download_report(nb.id, p, st.task_id)
                text = open(p).read()
            print("CHECK 2 report (built from source A = Podcast only):")
            print("  mentions 'lighthouse':", "lighthouse" in text.lower(), "(want False)")
            print("  first 400 chars:", text[:400].replace("\n", " "))

            url = client.notebooks.get_share_url(nb.id)
            if asyncio.iscoroutine(url):
                url = await url
            print("CHECK 5 open this on your phone; it should open the notebook in the NotebookLM app:", url)
            if keep:
                input("Press Enter after opening the link to delete the scratch notebook... ")
        finally:
            if not keep:
                await client.notebooks.delete(nb.id)
                print("deleted scratch notebook")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="pause for the phone check, then delete")
    asyncio.run(main(ap.parse_args().keep))
