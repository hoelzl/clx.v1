# %%
import asyncio
from clx.nb.notebook_processor import process_file

# %%
src = "C:/tmp/watcher_test/dir2/module-1.py"


# %%
async def test_process_file():
    await process_file(src, "dir2/module-1.py", "modified", "")


# %%
if __name__ == "__main__":
    asyncio.run(test_process_file())
