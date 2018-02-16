def on_create(session, target, path, **options):
    print("create", target)

def on_modify(session, target, path, **options):
    print("modify", target)

def to_resource(session, target, path, **options):
    return "demoResc"

def as_user(target, path, **options):
    return "tempZone", "rods"
