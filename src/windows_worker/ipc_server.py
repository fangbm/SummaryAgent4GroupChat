from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response

from pipeline_core.auth import verify_bearer, verify_image_signature
from pipeline_core.metrics import metrics_response
from pipeline_core.protocol import SignalMessage
from windows_worker.config import WorkerConfig
from windows_worker.task_processor import TaskProcessor


def create_app(config: WorkerConfig, processor: TaskProcessor) -> FastAPI:
    app = FastAPI(title="WeChat AI Pipeline Worker")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> Response:
        content, media_type = metrics_response()
        return Response(content=content, media_type=media_type)

    @app.get("/images/{filename}")
    def download_image(
        filename: str,
        expires: int = Query(...),
        sig: str = Query(...),
    ) -> FileResponse:
        if not verify_image_signature(filename, expires, sig, config.security.download_secret):
            raise HTTPException(status_code=403, detail="invalid or expired signature")
        root = Path(config.file_transfer.output_dir).resolve()
        target = (root / filename).resolve()
        if root not in target.parents and target != root:
            raise HTTPException(status_code=400, detail="invalid path")
        if not target.exists():
            raise HTTPException(status_code=404, detail="file not found")
        return FileResponse(target, media_type="image/png", filename=filename)

    @app.websocket(config.server.websocket_path)
    async def websocket_endpoint(websocket: WebSocket) -> None:
        if not verify_bearer(websocket.headers.get("authorization"), config.security.ipc_token):
            await websocket.close(code=4401)
            return
        await websocket.accept()

        async def send(signal: SignalMessage) -> None:
            await websocket.send_json(signal.model_dump(mode="json"))

        try:
            while True:
                data = await websocket.receive_json()
                signal = SignalMessage.model_validate(data)
                await processor.process_signal(signal, send)
        except WebSocketDisconnect:
            return

    return app
