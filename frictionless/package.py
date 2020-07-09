import os
import glob
from .metadata import Metadata
from .helpers import cached_property
from .resource import Resource
from . import helpers
from . import errors
from . import config


class Package(Metadata):
    """Package representation

    # Arguments
        descriptor? (str|dict): package descriptor

        profile? (str): profile
        resources? (dict[]): list of resource descriptors

    # Raises
        FrictionlessException: raise any error that occurs during the process

    """

    metadata_Error = errors.PackageError  # type: ignore
    metadata_setters = {
        "name": "name",
        "title": "title",
        "description": "description",
        "resources": "resources",
        "profile": "profile",
    }

    def __init__(
        self,
        descriptor=None,
        *,
        name=None,
        title=None,
        description=None,
        resources=None,
        profile=None,
        basepath=None,
    ):
        self.setinitial("name", name)
        self.setinitial("title", title)
        self.setinitial("description", description)
        self.setinitial("resources", resources)
        self.setinitial("profile", profile)
        self.__basepath = basepath or helpers.detect_basepath(descriptor)
        super().__init__(descriptor)

    @cached_property
    def name(self):
        return self.get("name")

    @cached_property
    def title(self):
        return self.get("title")

    @cached_property
    def description(self):
        return self.get("description")

    @cached_property
    def basepath(self):
        return self.__basepath

    # Resources

    @cached_property
    def resources(self):
        """Package's resources

        # Returns
            Resource[]: an array of resource instances

        """
        resources = self.get("resources", [])
        return self.metadata_attach("resources", resources)

    @cached_property
    def resource_names(self):
        """Schema's resource names

        # Returns
            str[]: an array of resource names

        """
        return [resource.name for resource in self.resources]

    def add_resource(self, descriptor):
        """ Add new resource to schema.

        The schema descriptor will be validated with newly added resource descriptor.

        # Arguments
            descriptor (dict): resource descriptor

        # Returns
            Resource/None: added `Resource` instance or `None` if not added

        """
        self.setdefault("resources", [])
        self["resources"].append(descriptor)
        return self.resources[-1]

    def get_resource(self, name):
        """Get resource by name.

        # Arguments
            name (str): resource name

        # Returns
           Resource/None: `Resource` instance or `None` if not found

        """
        for resource in self.resources:
            if resource.name == name:
                return resource
        return None

    def has_resource(self, name):
        """Check if a resource is present

        # Arguments
            name (str): schema resource name

        # Returns
           bool: whether there is the resource

        """
        for resource in self.resources:
            if resource.name == name:
                return True
        return False

    def remove_resource(self, name):
        """Remove resource by name.

        The schema descriptor will be validated after resource descriptor removal.

        # Arguments
            name (str): resource name

        # Returns
            Resource/None: removed `Resource` instances or `None` if not found

        """
        resource = self.get_resource(name)
        if resource:
            predicat = lambda resource: resource.name != name
            self["resources"] = list(filter(predicat, self.resources))
        return resource

    # Expand

    def expand(self):
        """Expand the package

        It will add default values to the package.

        """
        self.setdefault("resources", [])
        self.setdefault("profile", config.DEFAULT_PACKAGE_PROFILE)
        for resource in self.resources:
            resource.expand()

    # Infer

    def infer(self, *patterns):
        self.setdefault("profile", config.DEFAULT_PACKAGE_PROFILE)

        # From patterns
        if patterns:
            self.resources.clear()
            for pattern in patterns:
                options = {"recursive": True} if "**" in pattern else {}
                for path in glob.glob(os.path.join(self.basepath, pattern), **options):
                    self.resources.append({"path": os.path.relpath(path, self.basepath)})

        # General
        for resource in self.resources:
            resource.infer()

    # Save

    # TODO: zip metadata + data
    def save(self, target):
        self.metadata_save(target)

    # Metadata

    @cached_property
    def profile(self):
        return self.get("profile", config.DEFAULT_PACKAGE_PROFILE)

    @cached_property
    def metadata_profile(self):
        if self.profile == "tabular-data-package":
            return config.TABULAR_PACKAGE_PROFILE
        if self.profile == "fiscal-data-package":
            return config.FISCAL_PACKAGE_PROFILE
        return config.PACKAGE_PROFILE

    def metadata_process(self):

        # Resources
        resources = self.get("resources")
        if isinstance(resources, list):
            for index, resource in enumerate(resources):
                if not isinstance(resource, Resource):
                    if not isinstance(resource, dict):
                        resource = {"name": f"resource{index+1}"}
                    resource = Resource(resource, basepath=self.basepath)
                    list.__setitem__(resources, index, resource)
            if not isinstance(resources, helpers.ControlledList):
                resources = helpers.ControlledList(
                    resources, onchange=self.metadata_process
                )
                dict.__setitem__(self, "resources", resources)
