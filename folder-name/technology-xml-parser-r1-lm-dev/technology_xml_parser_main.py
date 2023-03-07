from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from google.cloud import storage
from sys import argv
from datetime import datetime, timedelta
from argparse import ArgumentParser
import os

BUCKET_NAME = 'vfgned-dh-{}-upm-landing' ## TOBEREPLACE
SPARK_JARS =  "../spark-xml/spark-xml_2.12-0.12.0.jar" ## TOBEREPLACE
BUCKET_OUTPUT = "gs://vfgned-dh-nwp-modeloutput/xml-parser/es/"

# Command line arguments
parser = ArgumentParser()
parser.add_argument('--StartDate', dest='startdate', required=True)
parser.add_argument('--EndDate', dest='enddate', required=True)
parser.add_argument("--lm", dest='lms')
parser.add_argument("--techs", dest='techs')
args = parser.parse_args()

# Parse CLI arguments
args, other = parser.parse_known_args(args = argv)



# Execution Parameters
startdate = args.startdate
enddate = args.enddate
lms = str(args.lms)
techs = str(args.techs)


# Convert to datetime format
sdate = datetime.strptime(startdate, '%Y%m%d')
edate = datetime.strptime(enddate, '%Y%m%d')

# Google Cloud Storage Client 
client = storage.Client()


# Spark Session
spark = (
    SparkSession.builder
    .appName('prarser_for_technology')
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.jars", SPARK_JARS) 
    .getOrCreate()
)



def build_paths(techs, sdate, edate):
    
    """ return a list of paths based on the type of 
        technology (2G,4G,5G) and a rage of time based 
        on the parameters sdate and edate """
    
    paths_list = []

    for tech in techs.split(","):
        prefix_im_provider = 'IM/provider/{}'.format(tech) # TOBEREPLACED
        delta = edate - sdate 
        cdate = sdate
        
        for i in range(delta.days + 1):

            year = str(cdate.year)
            month = str(cdate.month).zfill(2)
            day = str(cdate.day).zfill(2)

            if tech == '5G':
                tech_prefix = "/enmtbl1/"
                paths_list.append(prefix_im_provider + tech_prefix + f"year={year}/month={month}/day={day}")

            elif tech == '4G':
                tech_prefix = ""
                paths_list.append(prefix_im_provider + tech_prefix + f"/{year}{month}{day}")
                
            elif tech == '3G':
                tech_prefix = "/enmranleg1/"
                paths_list.append(prefix_im_provider + tech_prefix + f"year={year}/month={month}/day={day}")
                
            elif tech == '2G':
                tech_prefix = "/enmranbar1/"
                paths_list.append(prefix_im_provider + tech_prefix + f"year={year}/month={month}/day={day}")


            else:
                print("[UnsupportedTech] tech parameter/argument accepts only 2G, 4G or 5G accepted. Not {}. \n".format(tech))
                continue


            cdate = sdate + timedelta(days=i+1) 
    return paths_list


def read_sparkdf(blobname, bucketname=BUCKET_NAME):
    xml_file_path = "gs://"+ bucketname + "/" + blobname
    print(f"[Reading] from {xml_file_path}. \n")
    return spark.read \
            .format("xml").option("rowTag", "configData").load(xml_file_path)

def save_sparkdf_to_parquet(original_path, sparkdf, xml_bucket = BUCKET_OUTPUT):
    file_name = original_path.replace("gs://{BUCKET_NAME}/",xml_bucket)
    file_path = xml_bucket + os.path.dirname(file_name)
    print(f"[Saving] to {file_path}. \n") 
    sparkdf.write.parquet(file_path)    # .mode('overwrite')

    
def flat_2g_xml_file(sparkdf):
    return sparkdf.select(
                col("xn:SubNetwork._id").alias("SubNetworkId"),
                explode(col("xn:SubNetwork.xn:MeContext")).alias("MeContext")
    ).select(
            "SubNetworkId",
                col("MeContext._id").alias("MeContextId"),
                col("MeContext.xn:VsDataContainer._id").alias("ParentVsDataContainerId"),
                col("MeContext.xn:VsDataContainer.*"),
    ).select(
             "SubNetworkId","MeContextId", "ParentVsDataContainerId",
                col("xn:VsDataContainer.*"),
                col("xn:attributes.es:vsDataInventory").alias("ParentvsDataInventory"),
                col("xn:attributes.xn:vsDataFormatVersion").alias("ParentvsDataFormatVersion"),
                col("xn:attributes.xn:vsDataType").alias("ParentvsDataType")
    ).select(
            "SubNetworkId","MeContextId", "ParentVsDataContainerId","ParentvsDataInventory", 
            "ParentvsDataFormatVersion", "ParentvsDataType",
                col("_id").alias("MiddleVsDataContainerId"),
                col("xn:attributes.es:vsDataSoftwareInventory").alias("MiddlevsDataSoftwareInventory"),
                col("xn:attributes.xn:vsDataFormatVersion").alias("MiddlevsDataFormatVersion"),
                col("xn:attributes.xn:vsDataType").alias("MiddlevsDataType"),
                explode(col("xn:VsDataContainer"))
    ).select(
            "SubNetworkId","MeContextId", "ParentVsDataContainerId","ParentvsDataInventory", 
            "ParentvsDataFormatVersion", "ParentvsDataType","MiddleVsDataContainerId", "MiddlevsDataSoftwareInventory",
            "MiddlevsDataFormatVersion", "MiddlevsDataType",
                col("col._id").alias("ChildVsDataContainerId"),
                col("col.xn:attributes.xn:vsDataType").alias("ChildVsDataType"),
                col("col.xn:attributes.xn:vsDataFormatVersion").alias("ChildVsDataFormatVersion"),   
                col("col.xn:attributes.es:vsDataSoftwareItm.es:softwareItemId").alias("vsDataSoftwareItmSoftwareItemId"),    
                col("col.xn:attributes.es:vsDataSoftwareItm.es:type").alias("vsDataSoftwareItmSoftwareItemType"),   
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productDate").alias("vsDataSoftwareItmProductDate"),
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productDescription").alias("vsDataSoftwareItmProductDescription"),
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productName").alias("vsDataSoftwareItmProductName"),
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productNumber").alias("vsDataSoftwareItmProductNumber"),
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productRevision").alias("vsDataSoftwareItmProductRevision"),
                col("col.xn:attributes.es:vsDataSoftwareItm.es:administrativeData.es:productType").alias("vsDataSoftwareItmProductType"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:componentName").alias("vsDataSoftwareVerComponentName"),    
                col("col.xn:attributes.es:vsDataSoftwareVer.es:release").alias("vsDataSoftwareVerRelease"),    
                col("col.xn:attributes.es:vsDataSoftwareVer.es:softwareVersionId").alias("vsDataSoftwareVerSoftwareVersionId"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:timeOfActivation").alias("vsDataSoftwareVerTimeOfActivation"),    
                col("col.xn:attributes.es:vsDataSoftwareVer.es:timeOfInstallation").alias("vsDataSoftwareVerTimeOfInstallation"),        
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productDate").alias("vsDataSoftwareVerProductDate"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productDescription").alias("vsDataSoftwareVerProductDescription"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productName").alias("vsDataSoftwareVerProductName"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productNumber").alias("vsDataSoftwareVerProductNumber"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productRevision").alias("vsDataSoftwareVerProductRevision"),
                col("col.xn:attributes.es:vsDataSoftwareVer.es:administrativeData.es:productType").alias("vsDataSoftwareVerProductType")
            )

def flat_3g_xml_file(sparkdf):
    return sparkdf.select(
            col("xn:SubNetwork._id").alias("SubNetworkParentId"),
            col("xn:SubNetwork.xn:SubNetwork.xn:ManagedElement").alias("ManagedElement"),
                explode(col("xn:SubNetwork.xn:SubNetwork._id")).alias("SubNetworkChildId")
        ).select(
            "SubNetworkParentId","SubNetworkChildId",
                explode("ManagedElement")
        ).select(
            "SubNetworkParentId","SubNetworkChildId", 
                explode(col("col")).alias("ManagedElement")
        ).select(
            "SubNetworkParentId","SubNetworkChildId",
                col("ManagedElement._id").alias("ManagedElementId"),
                col("ManagedElement.xn:attributes.xn:managedElementType").alias("managedElementType"), 
                col("ManagedElement.xn:attributes.xn:userLabel").alias("managedElementUserLabel"), 
                col("ManagedElement.xn:attributes.xn:vendorName").alias("managedElementVendorName"),
                explode(col("ManagedElement.in:InventoryUnit")).alias("InventoryUnit")
        ).select(
                "SubNetworkParentId","SubNetworkChildId","ManagedElementId","managedElementType", "managedElementUserLabel", 
                                    "managedElementVendorName",
                col("InventoryUnit.*")
        ).select(
                "SubNetworkParentId","SubNetworkChildId","ManagedElementId","managedElementType", 
                "managedElementUserLabel", "managedElementVendorName",
                    col("_id").alias("InventoryUnitParentId"),  
                    col("in:attributes.in:dateOfManufacture").alias("InventoryUnitParentDateOfManufacture"), 
                    col("in:attributes.in:inventoryUnitType").alias("InventoryUnitParentInventoryUnitType"), 
                    col("in:attributes.in:manufacturerData").alias("InventoryUnitParentManufacturerData"), 
                    col("in:attributes.in:vendorName").alias("InventoryUnitParentVendorName"),
                    col("in:attributes.in:vendorUnitFamilyType").alias("InventoryUnitParentVendorUnitFamilyType"),
                    col("in:attributes.in:vendorUnitTypeNumber").alias("InventoryUnitParentVendorUnitTypeNumber"),
                    explode(col("in:InventoryUnit")).alias("InventoryUnit")
        ).select( 
                "SubNetworkParentId","SubNetworkChildId","ManagedElementId","managedElementType", "managedElementUserLabel", 
                "managedElementVendorName",
                "InventoryUnitParentId", "InventoryUnitParentDateOfManufacture","InventoryUnitParentInventoryUnitType", 
                "InventoryUnitParentManufacturerData", "InventoryUnitParentVendorName", "InventoryUnitParentVendorUnitFamilyType", 
                "InventoryUnitParentVendorUnitTypeNumber",
                    col("InventoryUnit._id").alias("InventoryUnitChildId"),  
                    col("InventoryUnit.in:attributes.in:dateOfManufacture").alias("InventoryUnitChildDateOfManufacture"), 
                    col("InventoryUnit.in:attributes.in:inventoryUnitType").alias("InventoryUnitChildInventoryUnitType"), 
                    col("InventoryUnit.in:attributes.in:manufacturerData").alias("InventoryUnitChildManufacturerData"), 
                    col("InventoryUnit.in:attributes.in:vendorName").alias("InventoryUnitChildVendorName"),
                    col("InventoryUnit.in:attributes.in:vendorUnitFamilyType").alias("InventoryUnitChildVendorUnitFamilyType"),
                    col("InventoryUnit.in:attributes.in:vendorUnitTypeNumber").alias("InventoryUnitChildVendorUnitTypeNumber")
                )
def flat_4g_xml_file(sparkdf):
    return sparkdf.select(
                col("xn:SubNetwork._id").alias("SubNetworkId"),
                explode(col("xn:SubNetwork.xn:ManagedElement")).alias("ManagedElement")
    ).select(
            "SubNetworkId", 
                col("ManagedElement._id").alias("ManagedElementId"),
                col("ManagedElement.xn:attributes.xn:managedElementType").alias("managedElementType"),
                col("ManagedElement.xn:attributes.xn:userLabel").alias("userLabel"),
                col("ManagedElement.xn:attributes.xn:vendorName").alias("vendorName"),
                explode(col("ManagedElement.in:InventoryUnit")).alias("InventoryUnitParent")
    ).select(
             "SubNetworkId","ManagedElementId", "managedElementType", "userLabel", "vendorName", 
                col("InventoryUnitParent._id").alias("InventoryUnitParentId"),
                col("InventoryUnitParent.in:attributes.in:dateOfManufacture").alias("InventoryUnitParentDateOfManufacture"),
                col("InventoryUnitParent.in:attributes.in:inventoryUnitType").alias("InventoryUnitParentInventoryUnitType"),
                col("InventoryUnitParent.in:attributes.in:manufacturerData").alias("InventoryUnitParentManufacturerData"),
                col("InventoryUnitParent.in:attributes.in:vendorName").alias("InventoryUnitParentVendorName"),
                col("InventoryUnitParent.in:attributes.in:vendorUnitFamilyType").alias("InventoryUnitParentVendorUnitFamilyType"),
                col("InventoryUnitParent.in:attributes.in:vendorUnitTypeNumber").alias("InventoryUnitParentVendorUnitTypeNumber"),
                explode(col("InventoryUnitParent.in:InventoryUnit")).alias("InventoryUnitChild")
).select(
                "SubNetworkId","ManagedElementId", "managedElementType", "userLabel", "vendorName", "InventoryUnitParentId",
                "InventoryUnitParentDateOfManufacture", "InventoryUnitParentInventoryUnitType", "InventoryUnitParentManufacturerData",
                "InventoryUnitParentVendorName",
                "InventoryUnitParentVendorUnitFamilyType","InventoryUnitParentVendorUnitTypeNumber",
                col("InventoryUnitChild._id").alias("InventoryUnitChildId"),
                col("InventoryUnitChild.in:attributes.in:dateOfManufacture").alias("InventoryUnitChildDateOfManufacture"),
                col("InventoryUnitChild.in:attributes.in:inventoryUnitType").alias("InventoryUnitChildInventoryUnitType"),
                col("InventoryUnitChild.in:attributes.in:manufacturerData").alias("InventoryUnitChildManufacturerData"),
                col("InventoryUnitChild.in:attributes.in:vendorName").alias("InventoryUnitChildVendorName"),
                col("InventoryUnitChild.in:attributes.in:vendorUnitFamilyType").alias("InventoryUnitChildVendorUnitFamilyType"),
                col("InventoryUnitChild.in:attributes.in:vendorUnitTypeNumber").alias("InventoryUnitChildVendorUnitTypeNumber"),
    )
   
def flat_5g_xml_file(sparkdf):
    return sparkdf.select(
                col("xn:SubNetwork._id").alias("SubNetworkId"),
                explode(col("xn:SubNetwork.xn:MeContext")).alias("MeContext")
    ).select(
            "SubNetworkId",
                col("MeContext._id").alias("MeContextId"),
                col("MeContext.xn:ManagedElement._id").alias("ManagedElementId"),
                col("MeContext.xn:ManagedElement.xn:VsDataContainer._id").alias("ParentVsDataContainerId"),

                col("MeContext.xn:ManagedElement.xn:VsDataContainer.*"),
    ).select(
             "SubNetworkId","ManagedElementId", "ParentVsDataContainerId",
                col("xn:VsDataContainer.*"),
                col("xn:attributes.es:vsDataSystemFunctions").alias("ParentvsDataSystemFunctions"),
                col("xn:attributes.xn:vsDataFormatVersion").alias("ParentvsDataFormatVersion"),
                col("xn:attributes.xn:vsDataType").alias("ParentvsDataType")
    ).select(
          "SubNetworkId","ManagedElementId", "ParentVsDataContainerId","ParentvsDataSystemFunctions", 
            "ParentvsDataFormatVersion", "ParentvsDataType",
                col("_id").alias("MiddleVsDataContainerId"),
                col("xn:attributes.es:vsDataSwInventory").alias("MiddlevsDataSwInventory"),
                col("xn:attributes.xn:vsDataFormatVersion").alias("MiddlevsDataFormatVersion"),
                col("xn:attributes.xn:vsDataType").alias("MiddlevsDataType"),
                explode(col("xn:VsDataContainer"))
    ).select(
          "SubNetworkId","ManagedElementId", "ParentVsDataContainerId","ParentvsDataSystemFunctions", 
            "ParentvsDataFormatVersion", "ParentvsDataType",
            "MiddleVsDataContainerId", "MiddlevsDataSwInventory",  "MiddlevsDataFormatVersion", "MiddlevsDataType",
                col("col._id").alias("ChildVsDataContainerId"),
                col("col.xn:attributes.xn:vsDataType").alias("ChildVsDataType"),
                col("col.xn:attributes.xn:vsDataFormatVersion").alias("ChildVsDataFormatVersion"),   
            
            col("col.xn:attributes.es:vsDataSwItem.es:additionalInfo").alias("vsDataSwItemAdditionalInfo"),  
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:type").alias("vsDataSwItemType"),   
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:description").alias("vsDataSwItemDescription"),
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:productName").alias("vsDataSwItemProductName"),
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:productNumber").alias("vsDataSwItemProductNumber"),
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:productRevision").alias("vsDataSwItemProductRevision"),
            col("col.xn:attributes.es:vsDataSwItem.es:administrativeData.es:productionDate").alias("vsDataSwItemProductDate"),
            col("col.xn:attributes.es:vsDataSwItem.es:consistsOf").alias("vsDataSwItemConsistsOf"),  
            col("col.xn:attributes.es:vsDataSwItem.es:swItemId").alias("vsDataSwItemSwItemId"),  
            col("col.xn:attributes.es:vsDataSwItem.es:userLabel").alias("vsDataSwItemUserLabel")
        )


# Main 
for lm in lms.split(','):
    
    # Set the source bucket based on Local Market 
    bucketName = BUCKET_NAME.format(lm)
    bucket = client.get_bucket(bucketName)
    
    # list the possible paths based on the parameters stated
    prefix_list = build_paths(techs, sdate, edate)
    
    for prefix in prefix_list:
        print("====New Path=======================\n")
        print(f"[CheckingFile] prefix {prefix}.\n")
        
        # validate if the file exists 
        if len(list(bucket.list_blobs(prefix=prefix)))>0: 
            blobs = bucket.list_blobs(prefix=prefix)

            for blob in blobs:
                # Read file 
                sparkdf = read_sparkdf(blob.name, bucketname=BUCKET_NAME)
                
                # use different functions based on the type of technology
                if "/5G/" in blob.name:
                    flat_sparkdf = flat_5g_xml_file(sparkdf)
                elif "/4G/" in blob.name:
                    flat_sparkdf = flat_4g_xml_file(sparkdf)
                elif "/3G/" in blob.name:
                    flat_sparkdf = flat_3g_xml_file(sparkdf)
                elif "/2G/" in blob.name:
                    flat_sparkdf = flat_2g_xml_file(sparkdf)
                else:
                    # write file to log 
                    print(f"[NoTechnologyInPath] No reference to technology suppeorted in path check file: {blob.name} \n")
                    continue 
                    
                # write the new file
                try:
                    save_sparkdf_to_parquet(blob.name, flat_sparkdf)
                    
                except Exception as e:
                    print(f"[AlreadyExist] Already Exist {str(e)}. \n")
                    
        else:
            print(f"[NotFound] path {prefix}. It is probably empty or doesn´t exist. \n")

