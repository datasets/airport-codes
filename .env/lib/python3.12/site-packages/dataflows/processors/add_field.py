from .add_computed_field import add_computed_field


def add_field(name, type, default=None, resources=None, **options):
    return add_computed_field(
        target=dict(
            name=name,
            type=type,
            **options
        ),
        resources=resources,
        operation=(
            default
            if callable(default) else
            (lambda row: default)
        )
    )
