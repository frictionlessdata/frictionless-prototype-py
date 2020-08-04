import stringcase
from importlib import import_module
from .metadata import Metadata
from . import exceptions
from . import helpers
from . import errors


class Pipeline(Metadata):
    """Pipeline representation

    """

    metadata_Error = errors.PipelineError
    metadata_profile = {  # type: ignore
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "steps": {
                "type": "array",
                "items": {"type": "object", "required": ["type", "spec"]},
            },
        },
    }

    def __init__(self, descriptor=None, *, name=None, type=None, steps=None):
        self.setinitial("name", name)
        self.setinitial("type", type)
        self.setinitial("steps", steps)
        super().__init__(descriptor)

    @Metadata.property
    def name(self):
        return self.get("name")

    @Metadata.property
    def type(self):
        return self.get("type")

    @Metadata.property
    def steps(self):
        return self.get("steps")

    # Run

    # NOTE: rebase on the plugin system
    def run(self):

        # Check type
        if self.type != "package":
            error = errors.Error(note='For now, the only supported type is "package"')
            raise exceptions.FrictionlessException(error)

        # Import dataflows
        try:
            dataflows = import_module("dataflows")
        except ImportError:
            error = errors.Error(note='Please install "frictionless[dataflows]"')
            raise exceptions.FrictionlessException(error)

        # Create flow
        items = []
        for step in self.steps:
            func = getattr(dataflows, stringcase.lowercase(step["type"]))
            items.append(func(**helpers.create_options(step["spec"])))
        flow = dataflows.Flow(*items)

        # Process flow
        flow.process()
