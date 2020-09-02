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
    Column('internal_key', BigInteger, primary_key=True, autoincrement=True),
    Column('informant', BigInteger, ForeignKey("activities.internal_key")),
    Column('informed', BigInteger, ForeignKey("activities.internal_key")))

################################################################################
# Define the Activity class mapped to the activities table
class Activity(provBase):
    __tablename__ = 'activities'
    ordered_attribute_list = ['id','name','startTime','endTime','comment',\
                              'activityDescription_id']
    other_display_attributes = ['name','comment']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    id        = Column(String)
    name      = Column(String)
    startTime = Column(String)
    endTime   = Column(String)
    comment   = Column(String)

    # n-1 relation with ActivityDescription
    activityDescription_key = Column(BigInteger, ForeignKey("activityDescriptions.internal_key"))
    activityDescription    = relationship("ActivityDescription")
    # n-n relation
    wasInformedBy = relationship('Activity',\
        secondary=wasInformedBy_association_table,
        primaryjoin=internal_key   == wasInformedBy_association_table.c.informed,
        secondaryjoin=internal_key == wasInformedBy_association_table.c.informant)

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

    def get_description_key(self):
        return self.activityDescription_Key

################################################################################
# wasDerivedFrom association table (n-n relation)
wasDerivedFrom_association_table = Table('wasDerivedFrom', provBase.metadata,
    Column('internal_key', BigInteger, primary_key=True),
    Column('generatedEntity', BigInteger, ForeignKey("entities.internal_key")),
    Column('usedEntity', BigInteger, ForeignKey("entities.internal_key")))

################################################################################
# Define the Entity class mapped to the entities table
class Entity(provBase):
    __tablename__ = 'entities'
    ordered_attribute_list = ['id','classType','name','location','generatedAtTime',\
                              'invalidatedAtTime','comment','entityDescription_id']
    other_display_attributes = ['id','name','location','generatedAtTime',\
                                'invalidatedAtTime','comment']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    id                  = Column(String)
    name                = Column(String)
    location            = Column(String)
    generatedAtTime     = Column(DateTime)
    invalidatedAtTime   = Column(DateTime)
    comment             = Column(String)

    # Heritage
    classType           = Column(String)
    __mapper_args__ = {
        'polymorphic_identity':'entity',
        'polymorphic_on': classType
    }

    # n-1 relation with EntityDescription
    entityDescription_key   = Column(BigInteger, ForeignKey("entityDescriptions.internal_key"))
    entityDescription      = relationship("EntityDescription")
    # n-n relation
    wasDerivedFrom = relationship('Entity',\
        secondary=wasDerivedFrom_association_table,
        primaryjoin=internal_key   == wasDerivedFrom_association_table.c.usedEntity,
        secondaryjoin=internal_key == wasDerivedFrom_association_table.c.generatedEntity)

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
# Define the collection class withy the hadMember: TO BE DONE!

################################################################################
# Define the ValueEntity class mapped to the valueEntities table
class ValueEntity(Entity):
    __tablename__ = 'valueEntities'
    # ordered_attribute_list = Entity.ordered_attribute_list+['value']
    ordered_attribute_list = Entity.ordered_attribute_list
    other_display_attributes = ['value']

    # Internal key
    internal_key = Column(BigInteger, ForeignKey('entities.internal_key'), primary_key=True)

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
    other_display_attributes = ['ctadirac_guid']

    # Internal key
    internal_key = Column(BigInteger, ForeignKey('entities.internal_key'), primary_key=True)

    # Model attributes
    # CTADIRAC attributes
    ctadirac_guid = Column(String)

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
    ordered_attribute_list = ['role', 'time', 'activity_key', 'entity_key', \
                              'usageDescription_key']
    other_display_attributes = ['role', 'time']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role = Column(String, nullable=True)
    time = Column(String)

    # n-1 relation with Activity
    activity_key = Column(BigInteger, ForeignKey('activities.internal_key'))
    activity = relationship("Activity", backref='used')
    # n-1 relation with Entity
    entity_key = Column(BigInteger, ForeignKey('entities.internal_key'))
    entity = relationship("Entity", backref='used')
    # n-1 relation with UsageDescription
    usageDescription_key = Column(BigInteger, ForeignKey('usageDescriptions.internal_key'))
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
    ordered_attribute_list = ['role', 'activity_key', 'entity_key']
    other_display_attributes = ['role']

    # Internal Key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role = Column(String, nullable=True)

    # n-1 relation with Activity
    activity_key = Column(BigInteger, ForeignKey('activities.internal_key'))
    activity = relationship("Activity")
    # 0..1-1 relation with Entity
    entity_key = Column(BigInteger, ForeignKey('entities.internal_key'))
    entity = relationship("Entity", uselist=False)
    # n-1 relation with GenerationDescription
    generationDescription_key = Column(BigInteger, ForeignKey('generationDescriptions.internal_key'))
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
    ordered_attribute_list = ['id', 'name', 'type', 'email', 'comment',\
                              'affiliation', 'phone', 'address','url']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    id   = Column(String)
    name = Column(String)
    type = Column(String)
    comment = Column(String)
    email = Column(String)
    affiliation = Column(String)
    phone = Column(String)
    address = Column(String)
    url = Column(String)

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "Agent.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

################################################################################
# Define the WasAssociatedWith class mapped to the wasAssociatedWith table
class WasAssociatedWith(provBase):
    __tablename__ = 'wasAssociatedWith'
    ordered_attribute_list = ['activity_key','agent_key','role']
    other_display_attributes = ['role']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role     = Column(String, nullable=True)

    # n-1 relation with Activity
    activity_key = Column(BigInteger, ForeignKey('activities.internal_key'))
    activity = relationship("Activity", backref='wasAssociatedWith')
    # n-1 relation with Agent
    agent_key = Column(BigInteger, ForeignKey('agents.internal_key'))
    agent    = relationship("Agent", backref='wasAssociatedWith')

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasAssociatedWith.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the WasAttributedTo class mapped to the wasAttributedTo table
class WasAttributedTo(provBase):
    __tablename__ = 'wasAttributedTo'
    ordered_attribute_list = ['entity_key','agent_key','role']
    other_display_attributes = ['role']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role     = Column(String, nullable=True)

    # n-1 relation with Entity
    entity_key = Column(BigInteger, ForeignKey('entities.internal_key'))
    entity    = relationship("Entity", backref='wasAttributedTo')
    # n-1 relation with Agent
    agent_key = Column(BigInteger, ForeignKey('agents.internal_key'))
    agent    = relationship("Agent", backref='wasAttributedTo')

    # Print method
    def __repr__(self):
        response = ""
        for attribute in self.ordered_attribute_list:
            response += "WasAttributedTo.%s=%s\n" % (attribute, self.__dict__[attribute])
        return response

    # Other methods
    def get_display_attributes(self):
        response = {}
        for attribute in self.other_display_attributes:
            response['voprov:' + attribute] = self.__dict__[attribute]
        return response

################################################################################
# Define the ActivityDescription class mapped to the activityDescriptions table
class ActivityDescription(provBase):
    __tablename__ = 'activityDescriptions'
    ordered_attribute_list = ['name', 'version', 'description', 'type', 'subtype', 'doculink']
    other_display_attributes = ['name', 'version', 'description', 'type', 'subtype', 'doculink']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

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
    ordered_attribute_list = ['name', 'type', 'description', 'doculink', 'classType']
    other_display_attributes = ['name', 'type', 'description', 'doculink', 'classType']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

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

    # Internal key
    internal_key = Column(BigInteger, ForeignKey('entityDescriptions.internal_key'), primary_key=True)

    # Model attributes
    valueType = Column(String)
    unit = Column(String)
    ucd = Column(String)
    utype = Column(String)

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

    # Internal key
    internal_key = Column(BigInteger, ForeignKey('entityDescriptions.internal_key'), primary_key=True)

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
    ordered_attribute_list = ['role', 'description', 'type', 'multiplicity', \
                              'activityDescription_key', 'entityDescription_key']
    # Key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role = Column(String, nullable=True)
    description = Column(String)
    type = Column(String)
    multiplicity = Column(Integer)

    # n-1 relation with ActivityDescription
    activityDescription_key = Column(BigInteger, ForeignKey('activityDescriptions.internal_key'))
    activityDescription = relationship("ActivityDescription")
    # n-1 relation with EntityDescription
    entityDescription_key = Column(BigInteger, ForeignKey('entityDescriptions.internal_key'))
    entityDescription = relationship("EntityDescription")

    # Print method
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
    ordered_attribute_list = ['role', 'description', 'type', \
                              'activityDescription_key', 'entityDescription_key']
    other_display_attributes = ['role', 'description', 'type', 'multiplicity']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    role = Column(String, nullable=True)
    description = Column(String)
    type = Column(String)
    multiplicity = Column(Integer)

    # Relations
    activityDescription_key = Column(BigInteger, ForeignKey('activityDescriptions.internal_key'))
    activityDescription = relationship("ActivityDescription")
    # n-1 relation with EntityDescription
    entityDescription_key = Column(BigInteger, ForeignKey('entityDescriptions.internal_key'))
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

    # Internal keya
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    artefactType = Column(String, nullable=True)

    # n-1 relation with Activity
    activity_key = Column(BigInteger, ForeignKey('activities.internal_key'))
    activity = relationship("Activity", backref='WasConfiguredBy', uselist=False)
    # 1-1 relation with ConfigFile
    configFile_key = Column(BigInteger, ForeignKey('configFiles.internal_key'))
    configFile = relationship("ConfigFile", uselist=False)
    # 1-1 relation with Parameter
    parameter_key = Column(BigInteger, ForeignKey('parameters.internal_key'))
    parameter = relationship("Parameter", uselist=False)

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
    ordered_attribute_list = ['name', 'value']
    other_display_attributes = ['name', 'value']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    name = Column(String)
    value = Column(String)

    # n-1 relation with ParameterDescription
    parameterDescription_key = Column(BigInteger, ForeignKey("parameterDescriptions.internal_key"))
    parameterDescription = relationship("ParameterDescription")

    # Print method
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
    ordered_attribute_list = ['name', 'location', 'comment']
    other_display_attributes = ['name', 'location', 'comment']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    name = Column(String)
    location = Column(String)
    comment = Column(String)

    # n-1 relation with ConfigFileDescription
    configFileDescription_key = Column(BigInteger, ForeignKey("configFileDescriptions.internal_key"))
    configFileDescription = relationship("ConfigFileDescription")

    # Print method
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
    ordered_attribute_list = ['name', 'valueType', 'description', \
                              'unit', 'ucd', 'utype', 'min', 'max', 'default', 'options', 'activityDescription_key']
    other_display_attributes = ['name', 'valueType', 'description', \
                                'unit', 'ucd', 'utype', 'min', 'max', 'default', 'options']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

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

    # n-1 relation with ActivityDescription
    activityDescription_key = Column(BigInteger, ForeignKey("activityDescriptions.internal_key"))
    activityDescription = relationship("ActivityDescription")

    # Print method
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
    ordered_attribute_list = ['name', 'contentType', 'description', 'activityDescription_key']
    other_display_attributes = ['name', 'contentType', 'description']

    # Internal key
    internal_key = Column(BigInteger, primary_key=True, autoincrement=True)

    # Model attributes
    name = Column(String)
    contentType = Column(String)
    description = Column(String)

    # n-1 relation with ActivityDescription
    activityDescription_key = Column(BigInteger, ForeignKey("activityDescriptions.internal_key"))
    activityDescription = relationship("ActivityDescription")

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
      res = session.query(func.max(provInstance.internal_key).label('internal_key')).one()
      session.commit()
      return S_OK({'internal_key': res.internal_key})
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

    try:
        res = self.getAgentKey(agent.id)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
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
    try:
        res = self.getActivityDescriptionKey(activityDesc.name, activityDesc.version)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
        return self._sessionAdd(row)

  def addDatasetDescription(self, rowDict):
    '''
      Add DatasetDescription
      :param rowDict:
      :return:
    '''

    datasetDesc = DatasetDescription()
    row = self._dictToObject(datasetDesc, rowDict)
    try:
        res = self.getEntityDescriptionKey(datasetDesc.name)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
        return self._sessionAdd(row)

  def addUsageDescription(self, rowDict):
    '''
      Add UsageDescription
      :param rowDict:
      :return:
    '''

    usageDesc = UsageDescription()
    row = self._dictToObject(usageDesc, rowDict)
    try:
        res = self.getUsageDescriptionKey(usageDesc.activityDescription_key, \
                                          usageDesc.entityDescription_key, \
                                          usageDesc.role)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
        return self._sessionAdd(row)

  def addGenerationDescription(self, rowDict):
    '''
      Add UsageDescription
      :param rowDict:
      :return:
    '''

    generationDesc = GenerationDescription()
    row = self._dictToObject(generationDesc, rowDict)
    try:
        res = self.getGenerationDescriptionKey(generationDesc.activityDescription_key, \
                                          generationDesc.entityDescription_key, \
                                          generationDesc.role)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
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
    try:
        res = self.getEntityDescriptionKey(valueDesc.name)['Value']['internal_key']
        return S_OK({"internal_key":res})
    except:
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
        agentIDs.append(instance.internal_key)
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
                          .filter( DatasetEntity.invalidatedAtTime == None) \
                          .one()
      return S_OK(datasetEntity.internal_key)
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def updateDatasetEntity(self, internal_key, invalidatedAtTime):
    '''
      Update DatasetEntity
      :param internal_key, invalidatedAtTime
      :return:
    '''

    session = self.sessionMaker_o()
    try:
      datasetEntity = session.update( DatasetEntity )\
                          .where ( DatasetEntity.internal_key == internal_key )\
                          .values ( invalidatedAtTime = invalidatedAtTime )
      session.commit()
      return S_OK()
    except e:
      return S_OK()
    finally:
      session.close()

  def getUsageDescription(self, activityDescription_key, role):
    '''
      Get UsageDescription
      :param activityDescription_key, role
      :return: usageDescription.internal_key
    '''

    session = self.sessionMaker_o()
    try:
      usageDescription = session.query( UsageDescription )\
                          .filter(UsageDescription.activityDescription_key == activityDescription_key, UsageDescription.role == role)\
                          .one()
      session.commit()
      return S_OK({'internal_key':usageDescription.internal_key, 'entityDescription_key':usageDescription.entityDescription_key})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getGenerationDescription(self, activityDescription_key, role):
    '''
      Get GenerationDescription
      :param activityDescription_key, role
      :return: generationDescription.internal_key
    '''

    session = self.sessionMaker_o()
    try:
      generationDescription = session.query( GenerationDescription )\
                          .filter(GenerationDescription.activityDescription_key == activityDescription_key)\
                          .filter(GenerationDescription.role == role)\
                          .one()
      session.commit()
      return S_OK({'internal_key':generationDescription.internal_key, 'entityDescription_key':generationDescription.entityDescription_key})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getParameterDescription(self, activityDescription_key, parameter_name):
    '''
      Get getParameterDescription
      :param activityDescription_key, parameter_name
      :return: parameterDescription.internal_key
    '''

    session = self.sessionMaker_o()
    try:
      parameterDescription = session.query( ParameterDescription )\
                          .filter(ParameterDescription.activityDescription_key == activityDescription_key, \
                                  ParameterDescription.name == parameter_name)\
                          .one()
      session.commit()
      return S_OK({'internal_key':parameterDescription.internal_key})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getConfigFileDescription(self, activityDescription_key, configFile_name):
    '''
      Get getConfigFileDescription
      :param activityDescription_key, configFile_name
      :return: configFileDescription.internal_key
    '''

    session = self.sessionMaker_o()
    try:
      configFileDescription = session.query( ConfigFileDescription )\
                          .filter(ConfigFileDescription.activityDescription_key == activityDescription_key, \
                                  ConfigFileDescription.name == configFile_name)\
                          .one()
      session.commit()
      return S_OK({'internal_key':configFileDescription.s})
    except NoResultFound, e:
      return S_OK()
    finally:
      session.close()

  def getActivityDescriptionKey(self, activityDescription_name, activityDescription_version):
      """
        Get getActivityDescriptionKey
        :param activityDescription_name, activityDescription_version
        :return: ActivityDescription.internal_key
      """
      session = self.sessionMaker_o()
      try:
          activityDescription_list = session.query(ActivityDescription) \
              .filter(ActivityDescription.name == activityDescription_name) \
              .filter(ActivityDescription.version == activityDescription_version) \
              .all()
          activityDescription_last = activityDescription_list[-1]
          return S_OK({'internal_key': activityDescription_last.internal_key})

      except NoResultFound, e:
          return S_OK()
      except :
          return S_OK()
      finally:
          session.close()

  def getEntityDescriptionKey(self, entityDescription_name):
      """
        Get getEntityDescriptionKey
        :param entityDescription_name
        :return: EntityDescription.internal_key
      """
      session = self.sessionMaker_o()
      try:
          entityDescription_list = session.query(EntityDescription)\
                              .filter(EntityDescription.name == entityDescription_name)\
                              .all()

          entityDescription_last = entityDescription_list[-1]
          return S_OK({'internal_key': entityDescription_last.internal_key})
      except NoResultFound, e:
          return S_OK()
      finally:
          session.close()

  def getUsageDescriptionKey(self, usageDescription_activity, usageDescription_entity, usageDescription_role):
      """
        Get getUsageDescriptionKey
        :param usageDescription_activity, usageDescription_entity, usageDescription_role
        :return: UsageDescription.internal_key
      """
      session = self.sessionMaker_o()
      try:
          usageDescription_list = session.query(UsageDescription) \
              .filter(UsageDescription.activityDescription_key == usageDescription_activity) \
              .filter(UsageDescription.entityDescription_key == usageDescription_entity) \
              .filter(UsageDescription.role == usageDescription_role) \
              .all()

          usageDescription_last = usageDescription_list[-1]
          return S_OK({'internal_key': usageDescription_last.internal_key})
      except NoResultFound, e:
          return S_OK()
      finally:
          session.close()

  def getGenerationDescriptionKey(self, generationDescription_activity, generationDescription_entity, generationDescription_role):
      """
        Get getGenerationDescriptionKey
        :param generationDescription_activity, generationDescription_entity, generationDescription_role
        :return: GenerationDescription.internal_key
      """
      session = self.sessionMaker_o()
      try:
          generationDescription_list = session.query(GenerationDescription) \
              .filter(GenerationDescription.activityDescription_key == generationDescription_activity) \
              .filter(GenerationDescription.entityDescription_key == generationDescription_entity) \
              .filter(GenerationDescription.role == generationDescription_role) \
              .all()

          generationDescription_last = generationDescription_list[-1]
          return S_OK({'internal_key': generationDescription_last.internal_key})
      except NoResultFound, e:
          return S_OK()
      finally:
          session.close()

  def getAgentKey(self, agent_id):
      """
        Get AgentKey
        :param agent_id
        :return: Agent.internal_key
      """
      session = self.sessionMaker_o()
      try:
          agent_list = session.query(Agent) \
              .filter(Agent.id == agent_id) \
              .all()
          agent_last = agent_list[-1]
          return S_OK({'internal_key': agent_last.internal_key})
      except NoResultFound, e:
          return S_OK()
      finally:
          session.close()
