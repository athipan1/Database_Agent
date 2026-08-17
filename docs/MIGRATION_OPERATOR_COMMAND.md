# Migration operator command

The release migration entrypoint is:

```bash
python -m scripts.apply_runtime_migrations
```

It is intended for deployment automation. Railway invokes the same command through `railway.json` before starting the new API release.
