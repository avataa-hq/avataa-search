import inspect
import os
import re
from importlib import import_module


def is_not_abstract(cls):
    return not bool(getattr(cls, "__abstractmethods__", False))


def get_package_name(directory, project_root):
    relative_path = os.path.relpath(directory, project_root)
    pattern = r"^(\.+)"
    replaced_path = relative_path.replace(os.path.sep, ".")
    cleared_path = re.sub(pattern=pattern, string=replaced_path, repl="")  # noqa
    return cleared_path


def get_all_ancestors(cls):
    return inspect.getmro(cls)[1:]


def left_only_children(classes: list) -> list:
    ancestors = set()
    for cls in classes:
        ancestors.update(get_all_ancestors(cls))

    children = []
    for cls in classes:
        if cls not in ancestors:
            children.append(cls)
    return children


def find_subclasses_in_directory(directory, base_class):
    """
    Dynamic collection of operators at application startup
    """
    subclasses = []

    project_root = os.getcwd()
    package = get_package_name(directory, project_root)

    for filename in os.listdir(directory):
        if filename.endswith(".py") and not filename.startswith("_"):
            module_name = filename[:-3]

            try:
                module = import_module(f"{package}.{module_name}")
            except Exception as e:
                print(f"Module import error {module_name}: {e}")
                continue

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, base_class)
                    and obj != base_class
                    and is_not_abstract(cls=obj)
                ):
                    subclasses.append(obj)

    children_subclasses = left_only_children(classes=subclasses)
    return frozenset(children_subclasses)
