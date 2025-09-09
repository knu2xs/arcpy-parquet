import arcpy

__all__ = ["deactivate_parameter"]


def deactivate_parameter(parameter: arcpy.Parameter) -> None:
    """
    Deactivate an ArcPy Toolbox Parameter object instance. This sets the following three properties of the parameter.

    .. code-block:: python

        parameter.parameterType = "Optional"
        parameter.enabled = False
        parameter.value = None

    Args:
        parameter: ``arcpy.Parameter`` to deactivate.
    """
    parameter.parameterType = "Optional"
    parameter.enabled = False
    parameter.value = None
    return