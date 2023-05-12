import { Quadstore } from 'quadstore'
import { ClassicLevel } from 'classic-level'
import { DataFactory } from 'rdf-data-factory'
import FileSystem from 'fs'
import { RdfXmlParser } from 'rdfxml-streaming-parser'
import { Engine } from 'quadstore-comunica'

import events from 'events'
import readline from 'readline'

import QuadHelper from './custome_node_modules/quad-helper/index.js'
import XmlHelper from './custome_node_modules/xml-helper/index.js'

// #region startup

const ClassicDB = new ClassicLevel('./DataBaseLocation/DataBase')
const QuadFactory = new DataFactory()

const QuadDB = new Quadstore( {backend: ClassicDB, dataFactory: QuadFactory} )
await QuadDB.open()

const QuadParser = new RdfXmlParser()

const dataSourcesArray = [
    './DataSourceLocation/IEEE13.xml',
    './DataSourceLocation/IEEE13_Assets.xml',
    './DataSourceLocation/IEEE37.CIMXML',
    './DataSourceLocation/IEEE123.CIMXML'
];

for (const dataSourcesIterator of dataSourcesArray){    
    await importDataSource(dataSourcesIterator)
};

async function importDataSource(dataSourceLocation) {    
    const dataContentStream = FileSystem.createReadStream(dataSourceLocation)
    const quadStream = QuadParser.import(dataContentStream)
    await QuadDB.putStream(quadStream)
}

// initialization of query engine 
// responsible for executing queries against Quadstore
const QueryEngine = new Engine(QuadDB)

// #endregion

// #region Map: class instances to their class type using instance ID

const mapInstancesIdsToClassType = new Map()

const classTypesQuery =        
    'PREFIX class: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>' +
    'select distinct ?ClassType where {' +        
        '?subject class:type ?ClassType .' +    
    '}';

const classTypesStream = await QueryEngine.queryBindings(classTypesQuery)
const classTypesArray = await classTypesStream.toArray()  

for (const classTypeIterator of classTypesArray){
    const classType = classTypeIterator.entries.hashmap.node.value.value
    await createInstancesIdsForClassTypeQuery(classType)
};

async function createInstancesIdsForClassTypeQuery(classType) {        
        
    const instancesIdsForClassTypeQuery =          
        'select ?subjectID where {' +        
        '?subjectID ?y <' + classType + '>' +   
        '}';          

    await executeMapInstancesIdsToClassType(
        instancesIdsForClassTypeQuery, 
        getClassTypeFromCIM(classType)
    );
};

async function executeMapInstancesIdsToClassType(
    instancesIdsForClassTypeQuery, 
    classType
) {
    const instancesIdsForClassTypeStream = 
        await QueryEngine.queryBindings(instancesIdsForClassTypeQuery);
    const instancesIdsForClassTypeArray = 
        await instancesIdsForClassTypeStream.toArray();                  
    await QuadHelper.mapInstancesToType(
        mapInstancesIdsToClassType,   //map
        classType,                    //key     == class type
        instancesIdsForClassTypeArray //entries == id of every class instacene for current class type
    );     
};

// #endregion

// #region Map: attributes to their class type

const mapClassAttributesToClassType = new Map()
const enumSet = new Set()

// go through all instances of current class type
for (const classType of mapInstancesIdsToClassType.keys()) {
    for (const instanceId of mapInstancesIdsToClassType.get(classType)) {   
        // query for every single attribute of current instance           
        const allQuadsByIdQuery =          
            'select * where {' +        
                '<' + instanceId +'> ?o ?p' +    
            '}';         
        await executeMapClassAttributesToClassType(classType, allQuadsByIdQuery)    
    };
};

async function executeMapClassAttributesToClassType(classType, allQuadsByIdQuery) {  
    const allQuadsByIdStream = await QueryEngine.queryBindings(allQuadsByIdQuery)
    const allQuadsByIdArray = await allQuadsByIdStream.toArray()
    await QuadHelper.mapAttributesToType(
        mapClassAttributesToClassType, //map
        classType,                     //key     == class type
        allQuadsByIdArray,             //entries == class attributes
        enumSet                         //enum class names
    );           
};

// #endregion

// #region read file and parse content

const fileEntries = []
await XmlHelper.readXmlFile(
    FileSystem, 
    readline, 
    events, 
    './DataSourceLocation/Diplomski-rdfs-augmented.xml',
    fileEntries
);

const fileEntriesMap = new Map() //key    = name of whats being described 
                                 //entrie = description itself
XmlHelper.mapDescriptions(    
    fileEntries.slice(1), //fileEntries[0] contains namespaces, not a description
    fileEntriesMap
);

// #endregion

// #region process data and generate RDFS

const classTypes = new Set()
const extractedAttributes = new Set()
XmlHelper.extractCommonAttributes(
    mapClassAttributesToClassType,
    classTypes,
    extractedAttributes
);

generateRDFS(XmlHelper
    .generateRDFS(
        fileEntries[0],                //first line containing namespaces
        fileEntriesMap,                //all descriptions fount in source file
        mapClassAttributesToClassType, //used to check if there are any missing descriptions
        classTypes,                    //search fileEntriesMap for descriptions of this class types
        extractedAttributes,           //search fileEntriesMap for descriptions of this attributes
        enumSet                        //search fileEntriesMap for descriptions of this data types
    )
);

// #endregion

// #region help functions

function getClassTypeFromCIM(CIMClassType) {
    // example of CIMClassType string: http://iec.ch/TC57/CIM100#VoltageLimit
    // split it on # character and return right part    
    return CIMClassType.split("#")[1]
};

// #endregion

// #region work with file

function generateRDFS(content) {
    FileSystem.writeFile(         
        './GeneratedFileLocation/GeneratedCIMRDFS.xml',
        content, 
        function (err) {
            if (err) 
                throw err;
            console.log('RDFS file is generated!')
        }
    );
};

// #endregion