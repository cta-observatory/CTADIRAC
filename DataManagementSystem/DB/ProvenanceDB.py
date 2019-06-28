""" DIRAC Provenance DB
"""

# imports
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker, class_mapper, relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, func, Table, Column, MetaData, ForeignKey, \
Integer, String, DateTime, Enum, BLOB, exc, BigInteger, distinct

# from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters

__RCSID__ = "$Id$"

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

  def addActivity(self, cta_activity):

    session = self.sessionMaker_o()

    #if not session.query(exists().where(Activity.id==cta_activity['activity_uuid'])): # for the tests
    current_activity = Activity(id=cta_activity['activity_uuid'])
    current_activity.name=cta_activity['activity_name']
    current_activity.startTime=cta_activity['start']['time_utc']
    current_activity.endTime=cta_activity['stop']['time_utc']
    current_activity.comment=''
    current_activity.activityDescription_id=cta_activity['activity_name']+'_'+cta_activity['system']['ctapipe_version']

    try:
      session.add(current_activity)
      # Association with the agent
      wAW = WasAssociatedWith()
      wAW.activity = cta_activity['activity_uuid']
      wAW.agent    = "CTAO"
      #wAW.role = ?
      session.add(wAW)
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


  def addAgent(self, agentDict):
    '''
      Add Agent
      :param agent:
      :return:
    '''
    session = self.sessionMaker_o()

    agent = Agent(id=agentDict['id'])
    agent.name = agentDict['name']
    agent.type = agentDict['type']

    try:
      session.add(agent)
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



