# API startup gate

After the pre-deploy migration succeeds, the API reads the canonical schema identity. It starts only when the name, version, and hash match the release. This check does not repair schema.
