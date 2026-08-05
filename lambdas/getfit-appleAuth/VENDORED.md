# Vendored dependencies

This function's deployment package bundles 9 vendored directories
(third-party libraries) that are intentionally NOT committed.

Rebuild before deploying:

    pip install -r requirements.txt -t .

See `function-config.json` for the deployed runtime and handler.
