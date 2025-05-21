import argparse
import uvicorn
from uvicorn.config import LOGGING_CONFIG
from src.app import cors as app  # type: ignore # noqa: F401

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--port", required=False, help="port", default="8000")
    ap.add_argument("-w", "--workers", required=False, help="number workers", default="1")
    ap.add_argument("-r", "--reload", required=False, help="reload", default="1")
    args = vars(ap.parse_args())
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=int(args["port"]),
        workers=int(args["workers"]),
        reload=bool(int(args["reload"])),
    )
