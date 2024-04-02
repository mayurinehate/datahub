def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""

    if field_path.startswith("[version=2.0]"):
        # this is a v2 field path
        tokens = [
            t
            for t in field_path.split(".")
            if not (t.startswith("[") or t.endswith("]"))
        ]
        path = ".".join(tokens)
        return path
    else:
        # not a v2, we assume this is a simple path
        return field_path


def get_simple_json_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . and [] path notation from the v2 field path"""

    if field_path.startswith("[version=2.0]"):
        # v2 field path
        field_path = field_path.replace("[version=2.0].", "").replace("[key=True].", "")
        field_components = field_path.split(".")
        json_field_components = []
        array_count = 0
        for field_component in field_components:
            if field_component == "[type=array]":
                array_count += 1
            elif field_component.startswith("[type="):
                continue
            elif not field_component.startswith("["):
                json_field_components.append(
                    field_component + "".join(["[]" for _ in range(array_count)])
                )
                array_count = 0
        return ".".join(json_field_components)
    else:
        # not a v2, we assume this is a simple path
        return field_path
