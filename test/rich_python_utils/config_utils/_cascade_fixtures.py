"""Mock classes for alias-file cascade tests.

Separate module so Hydra can locate them via import path.
"""


class MockBase:
    def __init__(self, model_name="default", permission_mode="default"):
        self.model_name = model_name
        self.permission_mode = permission_mode


class MockParent:
    def __init__(self, base_inferencer=None, max_iterations=5):
        self.base_inferencer = base_inferencer
        self.max_iterations = max_iterations
