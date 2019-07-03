# imports
import json
from types import StringTypes
# Import sqlachemy modules to create objects mapped with tables
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import sessionmaker, class_mapper, relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine, func, MetaData, \
Integer, String, DateTime, Enum, BLOB, exc, BigInteger, distinct

# Declare a declarative_base to map objets and tables
from sqlalchemy.ext.declarative import declarative_base

# from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters


provBase = declarative_base()

# Define the Activity class mapped to the activities table
class Activity(provBase):
    __tablename__ = 'activities'
    ordered_attribute_list = ['id','name','startTime','endTime','comment','activityDescription_id']
    id        = Column(String, primary_key=True)
    name      = Column(String)
    startTime = Column(String)
    endTime   = Column(String)
    comment   = Column(String)
    activityDescription_id = Column(String, ForeignKey("activityDescriptions.id"))
    activityDescription    = relationship("ActivityDescription")
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Activity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the Entity class mapped to the entities table
class Entity(provBase):
    __tablename__ = 'entities'
    ordered_attribute_list = ['id','classType','name','location','generatedAtTime','invalidatedAtTime','comment','entityDescription_id' ]
    id                  = Column(String, primary_key=True)
    name                = Column(String)
    location            = Column(String)
    generatedAtTime     = Column(String)
    invalidatedAtTime   = Column(String)
    comment             = Column(String)
    classType           = Column(String)
    entityDescription_id   = Column(String, ForeignKey("entityDescriptions.id"))
    entityDescription      = relationship("EntityDescription")
    __mapper_args__ = {
        'polymorphic_identity':'entity',
        'polymorphic_on': classType
    }
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Entity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the Used class mapped to the used table
class Used(provBase):
    __tablename__ = 'used'
    ordered_attribute_list = ['id','role','time','activity_id','entity_id']
    id = Column(Integer, primary_key=True, autoincrement=True)
    role     = Column(String, nullable=True)
    time     = Column(String)
    activity_id = Column(String, ForeignKey('activities.id'))
    activity = relationship("Activity")
    entity_id = Column(String, ForeignKey('entities.id'))
    entity = relationship("Entity")
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Used.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the WasGeneratedBy class mapped to the wasGeneratedBy table
class WasGeneratedBy(provBase):
    __tablename__ = 'wasGeneratedBy'
    ordered_attribute_list = ['id','role','activity_id','entity_id']
    id = Column(Integer, primary_key=True, autoincrement=True)
    role     = Column(String, nullable=True)
    activity_id = Column(String, ForeignKey('activities.id'))
    activity = relationship("Activity")
    entity_id = Column(String, ForeignKey('entities.id'))
    entity = relationship("Entity")
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasGeneratedBy.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the ValueEntity class mapped to the valueEntities table
class ValueEntity(Entity):
    __tablename__ = 'valueEntities'
    #ordered_attribute_list = Entity.ordered_attribute_list+['value']
    ordered_attribute_list = Entity.ordered_attribute_list
    id = Column(String, ForeignKey('entities.id'), primary_key=True)
    valueXX = Column(String)
    __mapper_args__ = {'polymorphic_identity':'value'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ValueEntity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the DatasetEntity class mapped to the datasetEntities table
class DatasetEntity(Entity):
    __tablename__ = 'datasetEntities'
    ordered_attribute_list = Entity.ordered_attribute_list
    id = Column(String, ForeignKey('entities.id'), primary_key=True)
    __mapper_args__ = {'polymorphic_identity':'dataset'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "DatasetEntity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the Parameter class mapped to the parameters table
class Parameter(ValueEntity):
    __tablename__ = 'parameters'
    #ordered_attribute_list = Entity.ordered_attribute_list + ['valueType', 'unit', 'ucd', 'utype']
    ordered_attribute_list = Entity.ordered_attribute_list
    id = Column(String, ForeignKey('valueEntities.id'), primary_key=True)
    valueType = Column(String)
    unit      = Column(String)
    ucd       = Column(String)
    utype     = Column(String)
    __mapper_args__ = {'polymorphic_identity':'parameter'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Parameter.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the Agent class mapped to the agents table
class Agent(provBase):
    __tablename__ = 'agents'
    ordered_attribute_list = ['id','name','type','email','affiliation','phone','address']
    id                  = Column(String, primary_key=True)
    name                = Column(String)
    type                = Column(String)
    email               = Column(String)
    affiliation         = Column(String)
    phone               = Column(String)
    address             = Column(String)
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Agent.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the WasAssociatedWith class mapped to the wasAssociatedWith table
class WasAssociatedWith(provBase):
    __tablename__ = 'wasAssociatedWith'
    ordered_attribute_list = ['id','activity','agent','role']
    id       = Column(Integer, primary_key=True, autoincrement=True)
    activity = Column(String, ForeignKey("activities.id"))
    agent    = Column(String, ForeignKey("agents.id"))
    role     = Column(String, nullable=True)
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasAssociatedWith.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the WasAttributedTo class mapped to the wasAttributedTo table
class WasAttributedTo(provBase):
    __tablename__ = 'wasAttributedTo'
    ordered_attribute_list = ['id','entity','agent','role']
    id       = Column(Integer, primary_key=True, autoincrement=True)
    entity   = Column(String, ForeignKey("entities.id"))
    agent    = Column(String, ForeignKey("agents.id"))
    role     = Column(String, nullable=True)
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasAttributedTo.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the ActivityDescription class mapped to the activityDescriptions table
class ActivityDescription(provBase):
    __tablename__ = 'activityDescriptions'
    ordered_attribute_list = ['id','name','activity_type','activity_subtype','version','doculink']
    id                 = Column(String, primary_key=True)
    name               = Column(String)
    activity_type      = Column(String)
    activity_subtype   = Column(String)
    version            = Column(String)
    doculink           = Column(String)
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ActivityDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the EntityDescription class mapped to the entityDescriptions table
class EntityDescription(provBase):
    __tablename__ = 'entityDescriptions'
    ordered_attribute_list = ['id','name','type','description','doculink','classType']
    id                 = Column(String, primary_key=True)
    name               = Column(String)
    type               = Column(String)
    description        = Column(String)
    doculink           = Column(String)
    classType         = Column(String)
    __mapper_args__ = {
        'polymorphic_identity':'entityDescription',
        'polymorphic_on':classType
    }
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "EntityDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the UsageDescription class mapped to the usageDescriptions table
class UsageDescription(provBase):
    __tablename__ = 'usageDescriptions'
    ordered_attribute_list = ['id','role','description','type','activityDescription_id','entityDescription_id']
    id = Column(String, primary_key=True)
    role        = Column(String, nullable=True)
    description = Column(String)
    type        = Column(String)
    activityDescription_id = Column(String, ForeignKey('activityDescriptions.id'))
    activityDescription = relationship("ActivityDescription")
    entityDescription_id = Column(String, ForeignKey('entityDescriptions.id'))
    entityDescription = relationship("EntityDescription")
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "UsageDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the GenerationDescription class mapped to the generationDescriptions table
class GenerationDescription(provBase):
    __tablename__ = 'generationDescriptions'
    ordered_attribute_list = ['id','role','description','type','activityDescription_id','entityDescription_id']
    id          = Column(String, primary_key=True)
    role        = Column(String, nullable=True)
    description = Column(String)
    type        = Column(String)
    # Relations
    activityDescription_id = Column(String, ForeignKey('activityDescriptions.id'))
    activityDescription = relationship("ActivityDescription")
    entityDescription_id = Column(String, ForeignKey('entityDescriptions.id'))
    entityDescription = relationship("EntityDescription")
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "GenerationDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the ValueDescription class mapped to the valueDescriptions table
class ValueDescription(EntityDescription):
    __tablename__ = 'valueDescriptions'
    #ordered_attribute_list = EntityDescription.ordered_attribute_list + ['valueType','unit','ucd','utype']
    ordered_attribute_list = EntityDescription.ordered_attribute_list
    id = Column(String, ForeignKey('entityDescriptions.id'), primary_key=True)
    valueType = Column(String)
    unit      = Column(String)
    ucd       = Column(String)
    utype     = Column(String)
    __mapper_args__ = {'polymorphic_identity':'valueDescription'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ValueDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the DatasetDescription class mapped to the datasetDescriptions table
class DatasetDescription(EntityDescription):
    __tablename__ = 'datasetDescriptions'
    #ordered_attribute_list = EntityDescription.ordered_attribute_list + ['contentType']
    ordered_attribute_list = EntityDescription.ordered_attribute_list
    id = Column(String, ForeignKey('entityDescriptions.id'), primary_key=True)
    contentType = Column(String)
    __mapper_args__ = {'polymorphic_identity':'datasetDescription'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "DatasetDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

# Define the ParameterDescription class mapped to the parameterDescriptions table
class ParameterDescription(ValueDescription):
    __tablename__ = 'parameterDescriptions'
    #ordered_attribute_list = ValueDescription.ordered_attribute_list + ['min','max','options','default']
    ordered_attribute_list = EntityDescription.ordered_attribute_list
    id = Column(String, ForeignKey('valueDescriptions.id'), primary_key=True)
    min     = Column(String)
    max     = Column(String)
    options = Column(String)
    default = Column(String)
    __mapper_args__ = {'polymorphic_identity':'parameterDescription'}
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ParameterDescription.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

class ProvenanceDB( object ):
  '''
    Class that defines the interactions with the tables of the ProvenanceDB.
  '''

  def __getDBConnectionInfo( self, fullname ):
    """ Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
    """

    result = getDBParameters( fullname )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result[ 'Message' ] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]

  def __init__( self ):
    """c'tor
    :param self: self reference
    """

    self.log = gLogger.getSubLogger( 'ProvenanceDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'DataManagement/ProvenanceDB' )

    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'postgresql://%s:%s@%s:%s/%s' % ( self.dbUser,
                                                              self.dbPass,
                                                              self.dbHost,
                                                              self.dbPort,
                                                              self.dbName ),
                                 echo = runDebug )

    self.sessionMaker_o = sessionmaker(bind=self.engine)
    self.inspector = Inspector.from_engine(self.engine)

    #These are the list of tables that will be created.
    self.__initializeDB()

  def __initializeDB(self):
    """
    Create the tables, if they are not there yet
    """

    # sqlalchemy creates the database for me
    provBase.metadata.create_all(self.engine)

  def _sessionAdd(self, provInstance):

    session = self.sessionMaker_o()
    try:
      session.add(provInstance)
      session.commit()
      return S_OK()
    except exc.IntegrityError as err:
      self.log.warn("insert: trying to insert a duplicate key? %s" % err)
      session.rollback()
    except exc.SQLAlchemyError as e:
      session.rollback()
      self.log.exception("insert: unexpected exception", lException=e)
      return S_ERROR("insert: unexpected exception %s" % e)
    finally:
      session.close()

  def _dictToObject(self, table, fromDict):
    '''
      Add Agent
      :param agentDict:
      :return:
    '''


    fromDict = fromDict if isinstance( fromDict, dict )\
             else json.loads( fromDict ) if isinstance( fromDict, StringTypes )\
              else {}

    for key, value in fromDict.items():
      # The JSON module forces the use of UTF-8, which is not properly
      # taken into account in DIRAC.
      # One would need to replace all the '== str' with 'in StringTypes'
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( table, key, value )

    return table

  def addAgent(self, rowDict):
    '''
      Add Agent
      :param rowDict:
      :return:
    '''

    agent = Agent()
    row = self._dictToObject(agent, rowDict)

    return self._sessionAdd(row)

  def addActivity(self, rowDict):
    '''
      Add Activity
      :param rowDict:
      :return:
    '''

    activity = Activity()
    row = self._dictToObject(activity, rowDict)

    return self._sessionAdd(row)

  def addWasAssociatedWith(self, rowDict):
    '''
      Add WasAssociatedWith
      :param rowDict:
      :return:
    '''

    wasAssociatedWith = WasAssociatedWith()
    row = self._dictToObject(wasAssociatedWith, rowDict)

    return self._sessionAdd(row)

  def addActivityDescription(self, rowDict):
    '''
      Add ActivityDescription
      :param rowDict:
      :return:
    '''

    activityDesc = ActivityDescription()
    row = self._dictToObject(activityDesc, rowDict)
    return self._sessionAdd(row)

  def addDatasetDescription(self, rowDict):
    '''
      Add DatasetDescription
      :param rowDict:
      :return:
    '''

    datasetDesc = DatasetDescription()
    row = self._dictToObject(datasetDesc, rowDict)
    return self._sessionAdd(row)

  def addUsageDescription(self, rowDict):
    '''
      Add UsageDescription
      :param rowDict:
      :return:
    '''

    usageDesc = UsageDescription()
    row = self._dictToObject(usageDesc, rowDict)
    return self._sessionAdd(row)

  def addGenerationDescription(self, rowDict):
    '''
      Add UsageDescription
      :param rowDict:
      :return:
    '''

    generationDesc = GenerationDescription()
    row = self._dictToObject(generationDesc, rowDict)
    return self._sessionAdd(row)

  def addDatasetEntity(self, rowDict):
    '''
      Add DatasetEntity
      :param rowDict:
      :return:
    '''

    datasetEntity = DatasetEntity()
    row = self._dictToObject(datasetEntity, rowDict)
    return self._sessionAdd(row)

  def addWasAttributedTo(self, rowDict):
    '''
      Add WasAttributedTo
      :param rowDict:
      :return:
    '''

    wasAttributedTo = WasAttributedTo()
    row = self._dictToObject(wasAttributedTo, rowDict)
    return self._sessionAdd(row)

  def addUsed(self, rowDict):
    '''
      Add Used
      :param rowDict:
      :return:
    '''

    used = Used()
    row = self._dictToObject(used, rowDict)
    return self._sessionAdd(row)

  def addWasGeneratedBy(self, rowDict):
    '''
      Add WasGeneratedBy
      :param rowDict:
      :return:
    '''

    wasGeneratedBy = WasGeneratedBy()
    row = self._dictToObject(wasGeneratedBy, rowDict)
    return self._sessionAdd(row)

  def addValueEntity(self, rowDict):
    '''
      Add ValueEntity
      :param rowDict:
      :return:
    '''

    valueEntity = ValueEntity()
    row = self._dictToObject(valueEntity, rowDict)
    return self._sessionAdd(row)


  def addValueDescription(self, rowDict):
    '''
      Add ValueDescription
      :param rowDict:
      :return:
    '''

    valueDesc = ValueDescription()
    row = self._dictToObject(valueDesc, rowDict)
    return self._sessionAdd(row)

  def getAgents(self):
    '''
      Get Agents
      :return:
    '''

    session = self.sessionMaker_o()
    agentIDs = []
    try:
      for instance in session.query(Agent):
        agentIDs.append(instance.id)
      session.commit()
      return S_OK(agentIDs)
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()



