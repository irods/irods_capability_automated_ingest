from os.path import dirname, basename
from irods.models import Collection, DataObject

def size(session, path, replica_num = None, resc_name = None):
    args = [Collection.name == dirname(path), DataObject.name == basename(path)]
    
    if replica_num is not None:
        args.append(DataObject.replica_number == replica_num)
        
    if resc_name is not None:
        args.append(DataObject.resource_name == resc_name)
        
    for row in session.query(DataObject.size).filter(*args):
        return int(row[DataObject.size])
