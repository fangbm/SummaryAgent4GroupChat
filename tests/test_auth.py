import time

from pipeline_core.auth import sign_image_url, verify_bearer, verify_image_signature


def test_bearer_and_signed_url() -> None:
    assert verify_bearer("Bearer secret", "secret")
    assert not verify_bearer("Bearer other", "secret")

    url = sign_image_url("http://host/images", "a.png", "download-secret", 60)
    query = dict(part.split("=") for part in url.split("?")[1].split("&"))
    assert verify_image_signature(
        "a.png",
        int(query["expires"]),
        query["sig"],
        "download-secret",
        now=int(time.time()),
    )
    assert not verify_image_signature(
        "a.png",
        int(time.time()) - 1,
        query["sig"],
        "download-secret",
    )
