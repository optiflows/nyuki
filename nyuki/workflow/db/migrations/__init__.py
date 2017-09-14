import os
from importlib import util


async def run_migrations(*args, **kwargs):
    folder = os.path.dirname(__file__)
    migrations = set()
    with os.scandir(folder) as sd:
        for entry in sd:
            if entry.name.startswith('__') or entry.name.endswith('__.py'):
                continue
            migrations.add(entry.name)

    for filename in migrations:
        name, _ = os.path.splitext(filename)
        path = os.path.join(folder, filename)
        # Load module spec.
        spec = util.spec_from_file_location(name, path)
        # Load module.
        module = spec.loader.load_module(spec.name)
        # Instantiate module migration and run.
        migration = module.Migration(*args, **kwargs)
        await migration.run()
