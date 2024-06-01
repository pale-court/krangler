from .paths import Paths


class StateTracker:
    def __init__(self, paths: Paths, depot: int, manifest: int):
        self.paths = paths
        self.depot = depot
        self.manifest = manifest

    def has_state(self, state: str) -> bool:
        return self.paths.state_path(self.depot, self.manifest, state).exists()

    def set_state(self, state: str):
        p = self.paths.state_path(self.depot, self.manifest, state)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()

    def clear_state(self, state: str):
        p = self.paths.state_path(self.depot, self.manifest, state)
        p.unlink(missing_ok=True)
