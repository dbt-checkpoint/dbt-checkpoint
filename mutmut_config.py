def pre_mutation(context):  # type: ignore
    line = context.current_source_line.strip()
    if line.startswith("if __name__ =="):
        context.skip = True
    if line.startswith("help="):
        context.skip = True
    if line.startswith("metavar="):
        context.skip = True
    # Do not wont prints
    if line.startswith("print"):
        context.skip = True
    # Multiline prints
    if line.startswith('"'):
        context.skip = True
    # f-string Multiline prints
    if line.startswith('f"'):
        context.skip = True
    if line.startswith("default="):
        context.skip = True
    if line.startswith("nargs="):
        context.skip = True
    if line.startswith("required="):
        context.skip = True
    if line.startswith("@dataclass"):
        context.skip = True
