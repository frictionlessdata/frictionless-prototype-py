import os
import json
import glob
import zipfile
from copy import deepcopy
from .metadata import Metadata
from .resource import Resource
from . import exceptions
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

    metadata_duplicate = True
    metadata_Error = errors.PackageError  # type: ignore
    metadata_profile = deepcopy(config.PACKAGE_PROFILE)
    metadata_profile["properties"]["resources"] = {
        "type": "array",
        "items": {"type": "object"},
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
        trusted=None,
    ):

        # Handle zip
        if helpers.is_zip_descriptor(descriptor):
            descriptor = helpers.unzip_descriptor(descriptor, "datapackage.json")

        # Set attributes
        self.setinitial("name", name)
        self.setinitial("title", title)
        self.setinitial("description", description)
        self.setinitial("resources", resources)
        self.setinitial("profile", profile)
        self.__basepath = basepath or helpers.detect_basepath(descriptor)
        self.__trusted = trusted
        super().__init__(descriptor)

    @Metadata.property
    def name(self):
        return self.get("name")

    @Metadata.property
    def title(self):
        return self.get("title")

    @Metadata.property
    def description(self):
        return self.get("description")

    @Metadata.property(write=False)
    def basepath(self):
        return self.__basepath

    @Metadata.property
    def profile(self):
        return self.get("profile", config.DEFAULT_PACKAGE_PROFILE)

    # Resources

    @Metadata.property
    def resources(self):
        """Package's resources

        # Returns
            Resource[]: an array of resource instances

        """
        resources = self.get("resources", [])
        return self.metadata_attach("resources", resources)

    @Metadata.property(write=False)
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

    def infer(self, source=None, *, only_sample=False):
        self.setdefault("profile", config.DEFAULT_PACKAGE_PROFILE)

        # From source
        if source:
            self.resources.clear()
            if isinstance(source, str) and os.path.isdir(source):
                source = f"{source}/*"
            for pattern in source if isinstance(source, list) else [source]:
                options = {"recursive": True} if "**" in pattern else {}
                pattern = os.path.join(self.basepath, pattern)
                for path in sorted(glob.glob(pattern, **options)):
                    if path.endswith("package.json"):
                        continue
                    self.resources.append({"path": os.path.relpath(path, self.basepath)})

        # General
        for resource in self.resources:
            resource.infer(only_sample=only_sample)

    # Import/Export

    def to_dict(self, expand=False):
        result = super().to_dict()
        if expand:
            result = type(self)(result)
            result.expand()
            result = result.to_dict()
        return result

    # NOTE: support multipart
    def to_zip(self, target):
        try:
            with zipfile.ZipFile(target, "w") as zip:
                descriptor = self.copy()
                for resource in self.resources:
                    if resource.inline:
                        continue
                    if resource.remote:
                        continue
                    if resource.multipart:
                        continue
                    if not helpers.is_safe_path(resource.path):
                        continue
                    zip.write(resource.source, resource.path)
                descriptor = json.dumps(descriptor, indent=2, ensure_ascii=False)
                zip.writestr("datapackage.json", descriptor)
        except Exception as exception:
            error = errors.PackageError(note=str(exception))
            raise exceptions.FrictionlessException(error) from exception

    # Metadata

    def metadata_process(self):

        # Resources
        resources = self.get("resources")
        if isinstance(resources, list):
            for index, resource in enumerate(resources):
                if not isinstance(resource, Resource):
                    if not isinstance(resource, dict):
                        resource = {"name": f"resource{index+1}"}
                    resource = Resource(
                        resource,
                        basepath=self.__basepath,
                        trusted=self.__trusted,
                        package=self,
                    )
                    list.__setitem__(resources, index, resource)
            if not isinstance(resources, helpers.ControlledList):
                resources = helpers.ControlledList(resources)
                resources.__onchange__(self.metadata_process)
                dict.__setitem__(self, "resources", resources)

    def metadata_validate(self):
        yield from super().metadata_validate()

        # Extensions
        if self.profile == "fiscal-data-package":
            yield from super().metadata_validate(config.FISCAL_PACKAGE_PROFILE)

        # Resources
        for resource in self.resources:
            yield from resource.metadata_errors
