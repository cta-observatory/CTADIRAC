import json
# # from DIRAC
from DIRAC import S_OK, S_ERROR
# from CTADIRAC
from CTADIRAC.DataManagementSystem.private.JSONUtils import DMSEncoder


class ProvBase(object):

  def toJSON( self ):
    """ Returns the JSON formated string that describes the agent """
    try:
      jsonStr = json.dumps( self, cls = DMSEncoder )
      return S_OK( jsonStr )
    except Exception as e:
      return S_ERROR( str( e ) )

  def _jsonData( self, attrNames ):
    """ Returns the JSON formated string that describes the agent """
    jsonData = {}
    for attrName in attrNames :
      # id might not be set since it is managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue
      value = getattr( self, attrName )
      jsonData[attrName] = value

    return jsonData

class Activity(ProvBase):

  def __init__( self, id = None, name = None, startTime = None, \
                endTime = None, comment = None, activityDescription_key = None):

    self.id = id
    self.name = name
    self.startTime = startTime
    self.endTime = endTime
    self.comment = comment
    self.activityDescription_key = activityDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id','name','startTime','endTime','comment','activityDescription_key']
    return self._jsonData(attrNames)

class Entity(ProvBase):

  def __init__( self, id = None, classType = None, name = None, \
                location = None, generatedAtTime = None, \
                invalidatedAtTime = None, comment = None, entityDescription_key = None ):

    self.id = id
    self.classType = classType
    self.name = name
    self.location = location
    self.generatedAtTime = generatedAtTime
    self.invalidatedAtTime = invalidatedAtTime
    self.comment = comment
    self.entityDescription_key = entityDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id','classType','name','location','generatedAtTime','invalidatedAtTime','comment', \
                 'entityDescription_key' ]
    return self._jsonData(attrNames)

class ValueEntity(Entity):

  def __init__( self, id = None, classType = None, name = None, location = None, generatedAtTime = None, \
                invalidatedAtTime = None, comment = None, entityDescription_key = None, value = None ):

    Entity.__init__(self, id = id, name = name, location = location, generatedAtTime = generatedAtTime, \
                               invalidatedAtTime = invalidatedAtTime, comment = comment, \
                               entityDescription_key = entityDescription_key, classType = classType)
    self.value = value

class DatasetEntity(Entity):

  def __init__( self, id = None, classType = None, name = None, location = None,\
                generatedAtTime = None, invalidatedAtTime = None, comment = None, entityDescription_key = None,\
                ctadirac_guid = None):

    Entity.__init__(self,  id, classType, name, location, generatedAtTime, \
                 invalidatedAtTime, comment, entityDescription_key)
    self.ctadirac_guid = ctadirac_guid


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id','classType','name','location','generatedAtTime','invalidatedAtTime','comment', \
                 'entityDescription_key', 'value' ]
    return self._jsonData(attrNames)

class Used(ProvBase):

  def __init__( self, role = None, time = None, \
                activity_key = None, entity_key = None , usageDescription_key = None ):

    self.role = role
    self.time = time
    self.activity_key = activity_key
    self.entity_key = entity_key
    self.usageDescription_key = usageDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','time','activity_key','entity_key','usageDescription_key']
    return self._jsonData(attrNames)

class WasGeneratedBy(ProvBase):

  def __init__( self, id = None, role = None, activity_key = None, \
                entity_key = None, generationDescription_key = None ):

    self.role = role
    self.activity_key = activity_key
    self.entity_key = entity_key
    self.generationDescription_key = generationDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','activity_key','entity_key','generationDescription_key']
    return self._jsonData(attrNames)

class Agent(ProvBase):

  def __init__( self, id = None, name = None, type = None, email = None, \
                comment = None, affiliation = None, phone = None, \
                address = None, url = None):

    self.id = id
    self.name = name
    self.type = type
    self.email = email
    self.comment = comment
    self.affiliation = affiliation
    self.phone = phone
    self.address = address
    self.url = url

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ["id", "name", "type", "email", "comment", "affiliation", "phone", "address", "url"]
    return self._jsonData(attrNames)

class WasAttributedTo(ProvBase):

  def __init__( self, role = None , entity_key = None, agent_key = None):

    self.role = role
    self.entity_key = entity_key
    self.agent_key = agent_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','entity_key','agent_key']
    return self._jsonData(attrNames)

class WasAssociatedWith(ProvBase):

  def __init__( self, role = None, activity_key = None, agent_key = None):

    self.role = role
    self.activity_key = activity_key
    self.agent_key = agent_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','activity_key','agent_key']
    return self._jsonData(attrNames)

class ActivityDescription(ProvBase):

  def __init__(self, name=None, version=None, description=None, \
               type=None, subtype=None, doculink=None):
    self.name = name
    self.version = version
    self.description = description
    self.type = type
    self.subtype = subtype
    self.doculink = doculink

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'type', 'subtype', 'version', 'doculink', 'description']
    return self._jsonData(attrNames)

class EntityDescription(ProvBase):

  def __init__( self, name = None, type = None, description = None, doculink = None, classType = None):

    self.name = name
    self.type = type
    self.description = description
    self.doculink = doculink
    self.classType = classType

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id','name','type','description','doculink','classType']
    return self._jsonData(attrNames)

class DatasetDescription(EntityDescription):

  def __init__( self, name = None, type = None, description = None, doculink = None, classType = None, \
                contentType = None):

    EntityDescription.__init__(self, name = name, type = type, description = description, \
                               doculink = doculink, classType = classType)
    self.contentType = contentType

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'type', 'description', 'doculink', 'contentType']
    return self._jsonData(attrNames)

class ValueDescription(EntityDescription):

  def __init__( self, name = None, type = None, description = None, doculink = None, classType = None,\
                valueType = None, unit = None, ucd = None, utype = None):
    EntityDescription.__init__(self, name = name, type = type, description = description, \
                               doculink = doculink, classType = classType)
    self.valueType = valueType
    self.unit = unit
    self.ucd = ucd
    self.utype = utype

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'type', 'description', 'doculink', 'valueType', 'unit', 'ucd', 'utype']
    return self._jsonData(attrNames)

class UsageDescription(ProvBase):

  def __init__( self, role = None, description = None, type = None, multiplicity = 1, \
                activityDescription_key = None, entityDescription_key = None):

    self.role = role
    self.description = description
    self.type = type
    self.multiplicity = multiplicity
    self.activityDescription_key = activityDescription_key
    self.entityDescription_key = entityDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','description','type','multiplicity','activityDescription_key','entityDescription_key']
    return self._jsonData(attrNames)

class GenerationDescription(ProvBase):

  def __init__( self, role = None, description = None, type = None, multiplicity = 1, \
                activityDescription_key = None, entityDescription_key = None):

    self.role = role
    self.description = description
    self.type = type
    self.multiplicity = multiplicity
    self.activityDescription_key = activityDescription_key
    self.entityDescription_key = entityDescription_key

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['role','description','type','multiplicity','activityDescription_key','entityDescription_key']
    return self._jsonData(attrNames)

class WasConfiguredBy(ProvBase):

  def __init__(self, id=None, artefactType='Parameter', activity_key = None, parameter_key = None, configFile_key = None):

    self.id = id
    self.artefactType = artefactType
    self.activity_key = activity_key
    self.parameter_key = parameter_key
    self.configFile_key = configFile_key

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id', 'artefactType', 'activity_key', 'parameter_key', 'configFile_key']
    return self._jsonData(attrNames)

class Parameter(ProvBase):

  def __init__(self, id=None, value = None, name = None, parameterDescription_key = None):

    self.id = id
    self.value = value
    self.name = name
    self.parameterDescription_key = parameterDescription_key

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['id', 'value', 'name', 'parameterDescription_key']
    return self._jsonData(attrNames)

class ConfigFile(ProvBase):

  def __init__(self, name = None, location = None, comment = None, configFileDescription_key = None):

    self.name = name
    self.location = location
    self.comment = comment
    self.configFileDescription_key = configFileDescription_key

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'location', 'comment', 'configFileDescription_key']
    return self._jsonData(attrNames)

class ParameterDescription(ProvBase):

  def __init__(self, name = None, valueType = None, description = None, unit = None, ucd = None, \
               utype = None, min = None, max = None, default = None, options = None, activityDescription_key = None):

    self.name = name
    self.valueType = valueType
    self.description = description
    self.unit = unit
    self.ucd = ucd
    self.utype = utype
    self.min = min
    self.max = max
    self.default = default
    self.options = options
    self.activityDescription_key = activityDescription_key

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'valueType', 'description', 'unit', 'ucd', 'utype', 'min', 'max', 'default', 'options',\
                 'activityDescription_key']
    return self._jsonData(attrNames)

class ConfigFileDescription(ProvBase):

  def __init__(self, id = None, name = None, contentType = None, description = None, activityDescription_key = None):

    self.id = id
    self.name = name
    self.contentType = contentType
    self.description = description
    self.activityDescription_key = activityDescription_key

  def _getJSONData(self):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['name', 'contentType', 'description','activityDescription_key']
    return self._jsonData(attrNames)

