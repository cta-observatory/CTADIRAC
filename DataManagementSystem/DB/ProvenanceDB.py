# imports
import json
from types import StringTypes
# Import sqlachemy modules to create objects mapped with tables
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy import exists
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

################################################################################
# wasInformedBy association table (n-n relation)
wasInformedBy_association_table = Table('wasInformedBy', provBase.metadata,
    Column('wasInformedBy_Id', Integer, primary_key=True),
    Column('informant', String, ForeignKey("activities.id")),
    Column('informed', String, ForeignKey("activities.id")))

################################################################################
# Define the Activity class mapped to the activities table
class Activity(provBase):
    __tablename__ = 'activities'
    ordered_attribute_list = ['id','name','startTime','endTime','comment',\
                              'activityDescription_id']
    other_display_attributes = ['name','comment']

    # Model attributes included key
    id        = Column(String, primary_key=True)
    name      = Column(String)
    startTime = Column(String)
    endTime   = Column(String)
    comment   = Column(String)

    # n-1 relation with ActivityDescription
    activityDescription_id = Column(String, ForeignKey("activityDescriptions.id"))
    activityDescription    = relationship("ActivityDescription")
    # n-n relation
    wasInformedBy = relationship('Activity',\
        secondary=wasInformedBy_association_table,
        primaryjoin=id   == wasInformedBy_association_table.c.informed,
        secondaryjoin=id == wasInformedBy_association_table.c.informant)

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Activity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

    def get_description_id(self):
        return self.activityDescription_id

################################################################################
# Define the Entity class mapped to the entities table
# wasDerivedFrom association table (n-n relation)
wasDerivedFrom_association_table = Table('wasDerivedFrom', provBase.metadata,
    Column('wasDerivedFrom_Id', Integer, primary_key=True),
    Column('generatedEntity', String, ForeignKey("entities.id")),
    Column('usedEntity', String, ForeignKey("entities.id")))

################################################################################
# Define the Entity class mapped to the entities table
class Entity(provBase):
    __tablename__ = 'entities'
    ordered_attribute_list = ['id','classType','name','location','generatedAtTime',\
                              'invalidatedAtTime','comment','entityDescription_id']
    other_display_attributes = ['name','location','generatedAtTime',\
                                'invalidatedAtTime','comment']
    # Model attributes included key
    id                  = Column(String, primary_key=True)
    name                = Column(String)
    location            = Column(String)
    generatedAtTime     = Column(String)
    invalidatedAtTime   = Column(String)
    comment             = Column(String)

    # Heritage
    classType           = Column(String)
    __mapper_args__ = {
        'polymorphic_identity':'entity',
        'polymorphic_on': classType
    }

    # n-1 relation with EntityDescription
    entityDescription_id   = Column(String, ForeignKey("entityDescriptions.id"))
    entityDescription      = relationship("EntityDescription")
    # n-n relation
    wasDerivedFrom = relationship('Entity',secondary=wasDerivedFrom_association_table,
        primaryjoin=id   == wasDerivedFrom_association_table.c.usedEntity,
        secondaryjoin=id == wasDerivedFrom_association_table.c.generatedEntity)

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Entity.%s=%s\n" %(attribute,self.__dict__[attribute])
        return response
    # Other methods

    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response ['voprov:'+attribute]=self.__dict__[attribute]
        return response

################################################################################
# Define the ValueEntity class mapped to the valueEntities table
class ValueEntity(Entity):
    __tablename__ = 'valueEntities'
    # ordered_attribute_list = Entity.ordered_attribute_list+['value']
    ordered_attribute_list = Entity.ordered_attribute_list
    other_display_attributes = ['value']

    # Key
    id = Column(String, ForeignKey('entities.id'), primary_key=True)
    # Model attributes
    value = Column(String)

    # Heritage
    __mapper_args__ = {'polymorphic_identity': 'value'}

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ValueEntity.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

####################################################################################################
# Define the DatasetEntity class mapped to the datasetEntities table
class DatasetEntity(Entity):
    __tablename__ = 'datasetEntities'
    ordered_attribute_list = Entity.ordered_attribute_list
    other_display_attributes = []

    # Key
    id = Column(String, ForeignKey('entities.id'), primary_key=True)
    # Model attributes

    # Heritage
    __mapper_args__ = {'polymorphic_identity': 'dataset'}

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "DatasetEntity.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the Used class mapped to the used table
class Used(provBase):
    __tablename__ = 'used'
    ordered_attribute_list = ['id', 'role', 'time', 'activity_id', 'entity_id', \
                              'usageDescription_id']
    other_display_attributes = ['role', 'time']

    # Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Model attributes
    role = Column(String, nullable=True)
    time = Column(String)

    # n-1 relation with Activity
    activity_id = Column(String, ForeignKey('activities.id'))
    activity = relationship("Activity", backref='used')
    # n-1 relation with Entity
    entity_id = Column(String, ForeignKey('entities.id'))
    entity = relationship("Entity", backref='used')
    # n-1 relation with UsageDescription
    usageDescription_id = Column(String, ForeignKey('usageDescriptions.id'))
    usageDescription = relationship("UsageDescription")

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Used.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
# Define the WasGeneratedBy class mapped to the wasGeneratedBy table
class WasGeneratedBy(provBase):
    __tablename__ = 'wasGeneratedBy'
    ordered_attribute_list = ['id', 'role', 'activity_id', 'entity_id']
    other_display_attributes = ['role']

    # Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Model attributes
    role = Column(String, nullable=True)

    # n-1 relation with Activity
    activity_id = Column(String, ForeignKey('activities.id'))
    activity = relationship("Activity")
    # 0..1-1 relation with Entity
    entity_id = Column(String, ForeignKey('entities.id'))
    entity = relationship("Entity")
    # n-1 relation with GenerationDescription
    generationDescription_id = Column(String, ForeignKey('generationDescriptions.id'))
    generationDescription = relationship('GenerationDescription')

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasGeneratedBy.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the Agent class mapped to the agents table
class Agent(provBase):
    __tablename__ = 'agents'
    ordered_attribute_list = ['id', 'name', 'type', 'email', 'affiliation', 'phone', 'address']
    id = Column(String, primary_key=True)
    name = Column(String)
    type = Column(String)
    email = Column(String)
    affiliation = Column(String)
    phone = Column(String)
    address = Column(String)

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Agent.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

################################################################################
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

################################################################################
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

################################################################################
# Define the ActivityDescription class mapped to the activityDescriptions table
class ActivityDescription(provBase):
    __tablename__ = 'activityDescriptions'
    ordered_attribute_list = ['id', 'name', 'version', 'description', 'type', 'subtype', 'doculink']
    other_display_attributes = ['name', 'version', 'description', 'type', 'subtype', 'doculink']

    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    name = Column(String)
    version = Column(String)
    description = Column(String)
    type = Column(String)
    subtype = Column(String)
    doculink = Column(String)

    # 1-n relation with ConfigFileDescription
    configFiles = relationship("ConfigFileDescription")
    # 1-n relation with ParameterDescription
    parameters = relationship("ParameterDescription")

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ActivityDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the EntityDescription class mapped to the entityDescriptions table
class EntityDescription(provBase):
    __tablename__ = 'entityDescriptions'
    ordered_attribute_list = ['id', 'name', 'type', 'description', 'doculink', 'classType']
    other_display_attributes = ['name', 'type', 'description', 'doculink', 'classType']

    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    name = Column(String)
    type = Column(String)
    description = Column(String)
    doculink = Column(String)
    classType = Column(String)

    # Heritage
    __mapper_args__ = {
        'polymorphic_identity': 'entityDescription',
        'polymorphic_on': classType
    }

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "EntityDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the ValueDescription class mapped to the valueDescriptions table
class ValueDescription(EntityDescription):
    __tablename__ = 'valueDescriptions'
    # ordered_attribute_list = EntityDescription.ordered_attribute_list + ['valueType','unit','ucd','utype']
    ordered_attribute_list = EntityDescription.ordered_attribute_list
    other_display_attributes = []

    # Key
    id = Column(String, ForeignKey('entityDescriptions.id'), primary_key=True)
    # Model attributes
    valueType = Column(String)
    unit = Column(String)
    ucd = Column(String)
    utype = Column(String)
    min = Column(String)
    max = Column(String)
    default = Column(String)
    options = Column(String)

    # Heritage
    __mapper_args__ = {'polymorphic_identity': 'valueDescription'}

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ValueDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the DatasetDescription class mapped to the datasetDescriptions table
class DatasetDescription(EntityDescription):
    __tablename__ = 'datasetDescriptions'
    # ordered_attribute_list = EntityDescription.ordered_attribute_list + ['contentType']
    ordered_attribute_list = EntityDescription.ordered_attribute_list
    other_display_attributes = []

    # Key
    id = Column(String, ForeignKey('entityDescriptions.id'), primary_key=True)
    # Model attributes
    contentType = Column(String)

    # Heritage
    __mapper_args__ = {'polymorphic_identity': 'datasetDescription'}

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "DatasetDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the UsageDescription class mapped to the usageDescriptions table
class UsageDescription(provBase):
    __tablename__ = 'usageDescriptions'
    ordered_attribute_list = ['id', 'role', 'description', 'type', 'multiplicity', \
                              'activityDescription_id', 'entityDescription_id']
    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    role = Column(String, nullable=True)
    description = Column(String)
    type = Column(String)
    multiplicity = Column(Integer)

    # Relations
    activityDescription_id = Column(String, ForeignKey('activityDescriptions.id'))
    activityDescription = relationship("ActivityDescription")
    entityDescription_id = Column(String, ForeignKey('entityDescriptions.id'))
    entityDescription = relationship("EntityDescription")

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "UsageDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

####################################################################################################
# Define the GenerationDescription class mapped to the generationDescriptions table
class GenerationDescription(provBase):
    __tablename__ = 'generationDescriptions'
    ordered_attribute_list = ['id', 'role', 'description', 'type', \
                              'activityDescription_id', 'entityDescription_id']
    other_display_attributes = ['role', 'description', 'type', 'multiplicity']

    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    role = Column(String, nullable=True)
    description = Column(String)
    type = Column(String)
    multiplicity = Column(Integer)

    # Relations
    activityDescription_id = Column(String, ForeignKey('activityDescriptions.id'))
    activityDescription = relationship("ActivityDescription")
    entityDescription_id = Column(String, ForeignKey('entityDescriptions.id'))
    entityDescription = relationship("EntityDescription")

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "GenerationDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the WasConfiguredBy class mapped to the wasConfiguredBy table
class WasConfiguredBy(provBase):
    __tablename__ = 'wasConfiguredBy'
    ordered_attribute_list = ['id', 'artefactType']
    other_display_attributes = ['artefactType']

    # Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Model attributes
    artefactType = Column(String, nullable=True)

    # 1-n relation with Activity
    activity_id = Column(String, ForeignKey('activities.id'))
    activity = relationship("Activity", backref='WasConfiguredBy', uselist=False)
    # 1-1 relation with ConfigFile
    configFile_id = Column(String, ForeignKey('configFiles.id'))
    configFile = relationship("ConfigFile", backref='WasConfiguredBy', uselist=False)
    # 1-1 relation with Parameter
    parameter_id = Column(String, ForeignKey('parameters.id'))
    parameter = relationship("Parameter", backref='WasConfiguredBy')

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasConfiguredBy.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
# Define the Parameter class mapped to the parameters table
class Parameter(provBase):
    __tablename__ = 'parameters'
    ordered_attribute_list = ['id', 'name', 'value']
    other_display_attributes = ['name', 'value']

    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    name = Column(String)
    value = Column(String)

    # n-1 relation with ParameterDescription
    parameterDescription_id = Column(Integer, ForeignKey("parameterDescriptions.id"))
    parameterDescription = relationship("ParameterDescription")

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Parameter.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
# Define the ConfigFile class mapped to the configFiles table
class ConfigFile(provBase):
    __tablename__ = 'configFiles'
    ordered_attribute_list = ['id', 'name', 'location', 'comment']
    other_display_attributes = ['name', 'location', 'comment']

    # Key
    id = Column(String, primary_key=True)
    # Model attributes
    name = Column(String)
    location = Column(String)
    comment = Column(String)

    # n-1 relation with ConfigFileDescription
    configFileDescription_id = Column(Integer, ForeignKey("configFileDescriptions.id"))
    configFileDescription = relationship("ConfigFileDescription")

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ConfigFile.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
# Define the ParameterDescription class mapped to the parameters table
class ParameterDescription(provBase):
    __tablename__ = 'parameterDescriptions'
    ordered_attribute_list = ['id', 'name', 'valueType', 'description', \
                              'unit', 'ucd', 'utype', 'min', 'max', 'default', 'options', 'activityDescription_id']
    other_display_attributes = ['name', 'valueType', 'description', \
                                'unit', 'ucd', 'utype', 'min', 'max', 'default', 'options']

    # Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Model attributes
    name = Column(String)
    valueType = Column(String)
    description = Column(String)
    unit = Column(String)
    ucd = Column(String)
    utype = Column(String)
    min = Column(String)
    max = Column(String)
    default = Column(String)
    options = Column(String)

    # 1-n relation with ActivityDescription
    activityDescription_id = Column(String, ForeignKey("activityDescriptions.id"))

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ParameterDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
# Define the ConfigFileDescription class mapped to the configFileDescriptions table
class ConfigFileDescription(provBase):
    __tablename__ = 'configFileDescriptions'
    ordered_attribute_list = ['id', 'name', 'contentType', 'description', 'activityDescription_id']
    other_display_attributes = ['name', 'contentType', 'description']

    # Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Model attributes
    name = Column(String)
    contentType = Column(String)
    description = Column(String)

    # 1-n relation with ActivityDescription
    activityDescription_id = Column(String, ForeignKey("activityDescriptions.id"))

    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "ConfigFileDescription.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response


################################################################################
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
      return S_ERROR("Key already exists")
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

  def addWasConfiguredBy(self, rowDict):
    '''
      Add WasConfiguredBy
      :param rowDict:
      :return:
    '''

    wasConfiguredBy = WasConfiguredBy()
    row = self._dictToObject(wasConfiguredBy, rowDict)
    return self._sessionAdd(row)

  def addParameter(self, rowDict):
    '''
      Add Parameter
      :param rowDict:
      :return:
    '''

    parameter = Parameter()
    row = self._dictToObject(parameter, rowDict)
    return self._sessionAdd(row)

  def addConfigFile(self, rowDict):
    '''
      Add ConfigFile
      :param rowDict:
      :return:
    '''

    configFile = ConfigFile()
    row = self._dictToObject(configFile, rowDict)
    return self._sessionAdd(row)

  def addParameterDescription(self, rowDict):
    '''
      Add ParameterDescription
      :param rowDict:
      :return:
    '''

    parameterDescription = ParameterDescription()
    row = self._dictToObject(parameterDescription, rowDict)
    return self._sessionAdd(row)

  def addConfigFileDescription(self, rowDict):
    '''
      Add ConfigFileDescription
      :param rowDict:
      :return:
    '''

    configFileDescription = ConfigFileDescription()
    row = self._dictToObject(configFileDescription, rowDict)
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

  def getDatasetEntity(self, guid):
    '''
      Get DatasetEntity
      :param guid
      :return:
    '''

    session = self.sessionMaker_o()
    try:
      datasetEntity = session.query( DatasetEntity )\
                          .filter( DatasetEntity.id == guid ) \
                          .one()
      session.commit()
      return S_OK(datasetEntity.id)
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getUsageDescription(self, activityDescription_id, role):
    '''
      Get UsageDescription
      :param activity_id, role
      :return: usageDescription.id
    '''

    session = self.sessionMaker_o()
    try:
      usageDescription = session.query( UsageDescription )\
                          .filter(UsageDescription.activityDescription_id == activityDescription_id, UsageDescription.role == role)\
                          .one()
      session.commit()
      return S_OK({'id':usageDescription.id, 'entityDescription_id':usageDescription.entityDescription_id})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getGenerationDescription(self, activityDescription_id, role):
    '''
      Get GenerationDescription
      :param activity_id, role
      :return: generationDescription.id
    '''

    session = self.sessionMaker_o()
    try:
      generationDescription = session.query( GenerationDescription )\
                          .filter(GenerationDescription.activityDescription_id == activityDescription_id, GenerationDescription.role == role)\
                          .one()
      session.commit()
      return S_OK({'id':generationDescription.id, 'entityDescription_id':generationDescription.entityDescription_id})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getParameterDescription(self, activityDescription_id, parameter_name):
    '''
      Get getParameterDescription
      :param activity_id, parameter_name
      :return: parameterDescription.id
    '''

    session = self.sessionMaker_o()
    try:
      parameterDescription = session.query( ParameterDescription )\
                          .filter(ParameterDescription.activityDescription_id == activityDescription_id, \
                                  ParameterDescription.name == parameter_name)\
                          .one()
      session.commit()
      return S_OK({'id':parameterDescription.id})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getConfigFileDescription(self, activityDescription_id, configFile_name):
    '''
      Get getConfigFileDescription
      :param activity_id, configFile_name
      :return: configFileDescription.id
    '''

    session = self.sessionMaker_o()
    try:
      configFileDescription = session.query( ConfigFileDescription )\
                          .filter(ConfigFileDescription.activityDescription_id == activityDescription_id, \
                                  ConfigFileDescription.name == configFile_name)\
                          .one()
      session.commit()
      return S_OK({'id':configFileDescription.id})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()



