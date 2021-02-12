# mongo client for data analysis off project
import pymongo
from pymongo import MongoClient
from pprint import pprint
import argparse
import time
import re
import sys
import pandas as pd
import datetime
import itertools
import numpy as np

# ****************************************************************************************************************************** generic functions

# ---------------------------------------------------------
# purpose : epoch time to human readable 
# ---------------------------------------------------------

def readableTime(timeAsString):
  unix_timestamp  = int(timeAsString)
  utc_time = time.gmtime(unix_timestamp)
  #local_time = time.localtime(unix_timestamp)
  #return time.strftime("%Y-%m-%d %H:%M:%S", local_time)
  return time.strftime("%Y-%m-%d %H:%M:%S", utc_time)

# ---------------------------------------------------------
# purpose : dataframe generator 
# ---------------------------------------------------------

def slice_generator(df, chunk_size):
    current_row = 0
    total_rows = df.shape[0]
    while current_row < total_rows:
        yield df[current_row:current_row + chunk_size]
        current_row += chunk_size

# ****************************************************************************************************************************** specific interactive functions

# ---------------------------------------------------------
# purpose : prints user's documents
#           consolidate changes
# ---------------------------------------------------------

def showDocsPerUser(user):
    req0 = {'last_modified_by': user}
    req = { "$query": req0, "$orderby": { "last_modified_t" : 1 } }    
    #req = { "$query": req0 }    
    skipped = 0
    nbr = 0
    cons = {}
    cons2 = {}
    # some revisions not even printed as no last_modified_by eg 1030000042.10
    #for doc in coll.find(req).limit(requestLimit):
    for doc in coll.find(req):
      #print(doc["_id"])

      # 1047103651.1 with no product_name
      # 4409003026.7 with no change
      # 0203804906.11 with no diffs in change
      if doc.get("product_name","") and doc.get("change","") and doc["change"].get("diffs",""):
        print("{:<18}| {:<24}| {:<75}| {}".format(doc["_id"], readableTime(doc["last_modified_t"]), doc["product_name"], doc["change"]["diffs"]))

        for k in doc["change"]["diffs"].keys():
          for k2 in doc["change"]["diffs"][k].keys():
            cons[k+"/"+k2] = cons.get(k+"/"+k2, 0) + 1
        
        for k in doc["change"]["diffs"].keys():
          for k2 in doc["change"]["diffs"][k].keys():
            for value in doc["change"]["diffs"][k][k2]:
              cons2[k+"/"+k2+">"+value] = cons2.get(k+"/"+k2+">"+value, 0) + 1

        nbr+=1
      else:
        skipped+=1

    print("printed", nbr)     
    print("skipped", skipped)
    print("consolidated changes")     
    pprint(cons)
    print("consolidated changes detailed")     
    pprint(cons2)

# ---------------------------------------------------------
# purpose : prints a specific document 
# ---------------------------------------------------------

def showDoc(docId):
    if re.search("^\d+$", docId):
      idx = 0
      for doc in coll.find({"_id": { "$regex": docId }}):
        idx+=1
        print("------------")
        for k in sorted(doc.keys()):
          if (doc[k] and doc[k] not in undefinedValues) or args.all:
            print("{:<50}: {}".format(k, str(doc[k])))
      print(idx, "found") 

    elif re.search("^\d+\.\d+$", docId):
      doc = coll.find_one({'_id': docId})
      for k in sorted(doc.keys()):
          if (doc[k] and doc[k] not in undefinedValues) or args.all:
              print("{:<50}: {}".format(k, str(doc[k])))

    else:
      print("invalid id\n")
      sys.exit()

# ****************************************************************************************************************************** application functions

# -------------------------------------------------------------------------------------
# purpose    : purge collections
# collection : da_users, da_products, da_revs 
# -------------------------------------------------------------------------------------

def purgeCollections():

    print("removing collection da_users")
    db.da_users.drop()
    print("removing collection da_products")
    db.da_products.drop()
    print("removing collection da_revs")
    db.da_revs.drop()
    print("removing collection da_IAT")
    db.da_IAT.drop()
    print("removing collection da_categories")
    db.da_categories.drop()

# -------------------------------------------------------------------------------------
# purpose    : populates modifyCount based on last_modified_by
# collection : da_users
# -------------------------------------------------------------------------------------

def da_users_userModifier(toInsert=False):

  pipeline = [{ "$match": { "last_modified_by": { "$ne": { "$type": 10 } } } },\
              { "$match": { "last_modified_by": { "$exists": "true" } } },\
              { "$group": { "_id": "$last_modified_by", 
                            "modifyCount": { "$sum": 1 },
                            "latest_time:": { "$max":  "$last_modified_t" },
                            "oldest_time:": { "$min":  "$last_modified_t" }
                          } 
              }]
  
  print(">>>>>>>>> executing:\n", pipeline)
  if toInsert:
    #print("merging into da_users")
    #coll.aggregate(pipeline +[{ "$merge": { "into": "da_users" } }])

    print("copying into da_users")
    coll.aggregate(pipeline +[{ "$out": "da_users" }])
  else:
    return list(coll.aggregate(pipeline))

# -------------------------------------------------------------------------------------
# purpose    : records stats per revision
# collection : da_revs
# -------------------------------------------------------------------------------------

def da_revs_stats(toInsert=False):

  pipeline = [
    { "$project": {  "created_date": { "$toDate": { "$toLong": { "$multiply": ["$created_t", 1000] }}}, 
                     "created_date_2":
                         { "$let": {
                             "vars": {
                               "created_date": { "$toDate": { "$toLong": { "$multiply": ["$created_t", 1000] } }}
                             },  
                             "in": {
                               "$dateFromParts": { 
                                         "year": { "$year": "$$created_date" },
                                         "month": { "$month": "$$created_date" },
                                         "day": { "$dayOfMonth": "$$created_date" } } 
                             }   
                         }   
                     },  

                     "modified_date": { "$toDate": { "$toLong": { "$multiply": ["$last_modified_t", 1000] } }}, 
                     "modified_date_2":
                         { "$let": {
                             "vars": {
                               "modified_date": { "$toDate": { "$toLong": { "$multiply": ["$last_modified_t", 1000] } }}
                             },  
                             "in": {
                               "$dateFromParts": { 
                                         "year": { "$year": "$$modified_date" },
                                         "month": { "$month": "$$modified_date" },
                                         "day": { "$dayOfMonth": "$$modified_date" } } 
                             }   
                         }   
                     },  

                     "rev": 1,
                     "last_modified_by": 1,
                     "creator": 1
                  }   
    }
  ]


  print("executing: ", pipeline)
  if toInsert:
    print("creating da_revs")
    coll.aggregate(pipeline + [{ "$out" : "da_revs" }])
    print("creating indexes")
    db.da_revs.create_index([("created_date_2", pymongo.ASCENDING)])
    db.da_revs.create_index([("modified_date_2", pymongo.ASCENDING)])
    print("removing null value documents")
    db.da_revs.delete_many({ "created_date_2" : { "$type": 10 } })
    db.da_revs.delete_many({ "modified_date_2" : { "$type": 10 } })

  else:
    return list(coll.aggregate(pipeline))

# ------------------------------------------------------------------------
# purpose : da_types_contribution_per_user function dataframe cleaning 
# ------------------------------------------------------------------------

def da_types_contribution_per_user_cleaning(dataframe):
        
    #num_rev
    dataframe['num_rev'] = dataframe['_id'].apply(lambda x : re.search('[.]\d+',x).group()[1:])
    dataframe['num_rev'] = dataframe['num_rev'].astype('int')
    
    #code_produit
    dataframe['product_code'] = dataframe['_id'].apply(lambda x : re.sub('[.]\d+','',x))
    
    #NaN
    dataframe.dropna(axis = 0, inplace = True)
    
    #drop _id column
    dataframe.drop(columns = ['_id'],inplace = True)
    
    #reset index
    dataframe.reset_index(drop=True,inplace=True)
    
    #clean and modify change.userid
    dataframe['change'] = dataframe['change'].apply(lambda x : list(x.values())[0])
    dataframe.rename(columns = {'change':'userid'}, inplace = True)
    dataframe['userid'] = dataframe['userid'].astype('str')
    
    #replace 'None values' by 'openfoodfact-contributors' in userid column
    dataframe['userid'] = dataframe['userid'].apply(lambda x : 'openfoodfacts-contributors' if x == 'None' else x)
    
    return dataframe

# ---------------------------------------------------------
# purpose : returns specific userid list ids
# ---------------------------------------------------------

def userid(dataframe):
    list_userid = list(dataframe['userid'].unique())
    
    return list_userid

# ---------------------------------------------------------
# purpose : populates tags counters per users 
# ---------------------------------------------------------

def select_last_rev_user(dataframe):
    
    #select index of max num_rev
    idx = dataframe.groupby(['product_code'])['num_rev'].transform(max) == dataframe['num_rev']
    
    #select matching rows with max num_rev
    dataframe_new = dataframe[idx].sort_values(by='product_code')
    
    #reset index
    #dataframe_new.reset_index(drop=True,inplace=True)
    
    #fieldList = ["correctors_tags", "informers_tags", "photographers_tags", "editors_tags", "checkers_tags"]
    #dataframe_new = dataframe_new.rename(columns={"product_code":"_id"})
    #dataframe_new = dataframe_new.drop("num_rev", axis=1)
    
    column_correctors = dataframe_new["correctors_tags"].explode().dropna().to_list()
    
    column_informers = dataframe_new["informers_tags"].explode().dropna().to_list()
    
    column_photographers = dataframe_new["photographers_tags"].explode().dropna().to_list()

    column_editors = dataframe_new["editors_tags"].explode().dropna().to_list()
    
    column_checkers = dataframe_new["checkers_tags"].explode().dropna().to_list()

    
    list_userid= userid(dataframe)
    
    list_correction = []
    list_information = []
    list_photos=[]
    list_edition =[]
    list_check =[]
    
    for i in list_userid: 
        count_correction = column_correctors.count(i)
        list_correction.append(count_correction)
        
        count_information = column_informers.count(i)
        list_information.append(count_information)
        
        count_photos = column_photographers.count(i)
        list_photos.append(count_photos)
        
        count_edition = column_editors.count(i)
        list_edition.append(count_edition)
        
        count_check = column_checkers.count(i)
        list_check.append(count_check)
        
    
    dataframe_final = pd.DataFrame({"correction": list_correction,"information": list_information,"photos_uploaded" : list_photos,
                                 "edition":list_edition,"check":list_check, "_id":list_userid})
       
 
    return dataframe_final
    

# -------------------------------------------------------------------------------------
# purpose : count source category and type
# -------------------------------------------------------------------------------------

def count_source(a):
    dico={}
    source_name={}
    source_type={}

    list_out=list(itertools.chain(*a))
    list_unique=list(set(list_out))
    ###########################################################################
    #Version 1 Add prefix source_type or source_name to key name usefull with Grafana 7.4+
    ###########################################################################
    #for i in range(len(list_unique)):
    #    if "-" in list_unique[i]:
    #        source_name["src_name_"+list_unique[i]]=list_out.count(list_unique[i])
    #        m= re.search('^\w+', list_unique[i])
    #        keySourceType="src_type_"+m.group(0)
    #
    #        if keySourceType in source_type:
    #            source_type[keySourceType]=source_type[keySourceType]+list_out.count(list_unique[i])
    #        else:
    #            source_type[keySourceType]=list_out.count(list_unique[i])
    ##########################################################################            
    
    ###########################################################################
    #Version 2 No Add  prefix key name 
    ###########################################################################
    
    for i in range(len(list_unique)):
        if "-" in list_unique[i]:
            source_name[list_unique[i]]=list_out.count(list_unique[i])
            m= re.search('^\w+', list_unique[i])
            keySourceType=m.group(0)

            if keySourceType in source_type:
                source_type[keySourceType]=source_type[keySourceType]+list_out.count(list_unique[i])
            else:
                source_type[keySourceType]=list_out.count(list_unique[i])
        dico.update(source_type)
        dico.update(source_name)
            
    return dico

# -------------------------------------------------------------------------------------
# purpose : compute the number of source and categories source for each user 
# -------------------------------------------------------------------------------------

def da_user_sourceFreq():
    
    pipeline=[
        {
            '$group': {
                '_id': '$change.userid', 
                'data_sources': {
                    '$addToSet': '$data_sources_tags'
                }, 
                'rev': {
                    '$sum': 1
                }
            }
        },
        {"$sort":{"rev":-1}}
    ]

    resu=coll.aggregate(pipeline)


    liste_resu= list(coll.aggregate(pipeline))

    dataframe=pd.DataFrame(liste_resu)

    dataframe["data_sources"]=dataframe["data_sources"].apply(lambda x :count_source(x))

    df_source=pd.json_normalize(dataframe.data_sources)
    df_source.fillna(0,inplace=True)
    dataframe=dataframe.join(df_source)

    dataframe.drop(columns=["data_sources"], inplace=True)
    dataframe.drop(columns=["rev"], inplace=True)

    #dataframe.set_index("_id",inplace=True)
    
    
    return dataframe


# -------------------------------------------------------------------------------------
# purpose : compute the number of contribution per contribution type for each user 
# -------------------------------------------------------------------------------------

def da_source_contribution_per_user(toInsert=False, chunkSize=1000):
    projectStage = {
        "$project": {
          "correctors_tags":1,
          "photographers_tags":1,
          "informers_tags":1,
          "editors_tags":1,
          "checkers_tags":1,
          "change.userid":1
        }
    }

    pipeline = [projectStage]
    #pipeline = [projectStage, {"$limit": 100000}]

    print(">>>>>>>>> executing:\n", pipeline)
    user_type = list(coll.aggregate(pipeline))
    
    print("loading in dataframe")
    dataframe = pd.DataFrame.from_dict(user_type)
    
    print("dataframe cleaning")
    dataframe = da_types_contribution_per_user_cleaning(dataframe)
    print("populating tags counters per users")
    dataframe = select_last_rev_user(dataframe)

    print("populating sources per users")
    dataframe2=da_user_sourceFreq()
    
    print("merging both dataframes")
    dataframe_global=dataframe.merge(dataframe2,how="outer",left_on="_id",right_on="_id")
    dataframe_global.fillna(0,inplace=True)

    if toInsert:
        print("recording in db, chunkSize is", chunkSize)
        for df_chunk in slice_generator(dataframe_global, chunkSize):
              records = df_chunk.to_dict(orient='records')
              upserts=[ pymongo.UpdateOne({"_id":x["_id"]}, {'$setOnInsert':x}, upsert=True) for x in records]
              result = db.da_users.bulk_write(upserts)
              #db.da_users.insert_many(records)
        
    else:
        return dataframe_global.to_string()
   
# -------------------------------------------------------------------------------
# purpose : compute the number of users per contribution type for each product
# -------------------------------------------------------------------------------

def da_types_contribution_per_product(toInsert=False):
    
    projectStage = [
      { 
        "$group":
        { 
          "_id": "$code",
          "correctors_tags": { "$max":  "$correctors_tags" },
          "photographers_tags": { "$max":  "$photographers_tags" },
          "informers_tags": { "$max":  "$informers_tags" },
          "editors_tags": { "$max":  "$editors_tags" },
          "checkers_tags": { "$max":  "$checkers_tags" }
        }
      },
      { 
        "$project":
        { 
          "correctors": { "$size": { "$ifNull": [ "$correctors_tags", [] ] } },
          "photographers": { "$size": { "$ifNull": [ "$photographers_tags", [] ] } },
          "informers": { "$size": { "$ifNull": [ "$informers_tags", [] ] } },
          "editors": { "$size": { "$ifNull": [ "$editors_tags", [] ] } },
          "checkers": { "$size": { "$ifNull": [ "$checkers_tags", [] ] } }
        }
      }
    ]

    print("executing: ", projectStage) 
    if toInsert:
      print("creating da_products")
      coll.aggregate(projectStage + [{ "$out" : "da_products" }])
    else:
      return list(coll.aggregate(projectStage))
      

# -------------------------------------------------------------------------------
# purpose : compute the number of users per contribution type for each category
# -------------------------------------------------------------------------------

def da_types_contribution_per_category(toInsert=False):
    
    
    pipelineCategory = [
    {"$group":
      {
        "_id": "$code",
        "correctors_tags": { "$max":  "$correctors_tags" },
        "photographers_tags": { "$max":  "$photographers_tags" },
        "informers_tags": { "$max":  "$informers_tags" },
        "editors_tags": { "$max":  "$editors_tags" },
        "checkers_tags": { "$max":  "$checkers_tags" },
        "categories_tags": { "$max":  "$categories_tags" }
      }
    },
    {
    "$unwind" : {
      "path": "$categories_tags",
      "preserveNullAndEmptyArrays": False
    }},

    
    {"$project":
      {
        "correctors": { "$size": { "$ifNull": [ "$correctors_tags", [] ] } },
        "photographers": { "$size": { "$ifNull": [ "$photographers_tags", [] ] } },
        "informers": { "$size": { "$ifNull": [ "$informers_tags", [] ] } },
        "editors": { "$size": { "$ifNull": [ "$editors_tags", [] ] } },
        "checkers": { "$size": { "$ifNull": [ "$checkers_tags", [] ] } },
        "categories_tags":1 
      }
    }, 
    
 
    {"$group":
      {
        "_id": "$categories_tags",
        "correctors": { "$sum":  "$correctors" },
        "photographers": { "$sum":  "$photographers" },
        "informers": { "$sum":  "$informers" },
        "editors": { "$sum":  "$editors" },
        "checkers": { "$sum":  "$checkers" }
      }
    },

    {"$project":
      {
        "correctors": 1,
        "photographers": 1,
        "informers": 1,
        "editors": 1,
        "checkers": 1,
        "category":{"$substr": ["$_id",3,-1]},
        "_id" : 0
      }
    }
    ]


    creationCollStage = { "$out" : "da_categories" }

    print("executing: ", pipelineCategory) 
    if toInsert:
      print("creating da_categories")
      coll.aggregate(pipelineCategory + [creationCollStage])
    else:
      return list(coll.aggregate(pipelineCategory))

# -------------------------------------------------------------------------------
# purpose : get IAT status ratios and correlation coefficients with number of revs
# -------------------------------------------------------------------------------

def da_IAT_function(toInsert = False, chunkSize = 2000):

    data_retrieved = coll.find({},{ "rev": 1, "ingredients_analysis_tags": 1, "code" : 1})

    df_start = pd.DataFrame(list(data_retrieved))

    # # 1 - np.count_nonzero(df["ingredients_analysis_tags"].isnull()) / len(df) # filling rate for IAT column

    df_start.dropna(inplace = True) # dropping rows with NaN ( mostly rows with NaN in IAT, as rev and countries filling rates 
                                    # are respectively 100 and 99.8% in our subset)

    df_start.drop(df_start[df_start['ingredients_analysis_tags'].map(len) <3].index, inplace = True)   
        
    df_start.reset_index(drop = True, inplace = True)

    df_status = pd.DataFrame(df_start['ingredients_analysis_tags'].values.tolist(), columns = ['palm oil','vegan','vegetarian'])

    df_start.drop('ingredients_analysis_tags', axis = 1, inplace = True)

    df_fusion = pd.concat([df_start, df_status], axis = 1)
    
    df_fusion['rev'] = df_fusion['rev'].apply(lambda x: int(x))

    max_rev_id = df_fusion.groupby(['code'])['rev'].transform(max) == df_fusion['rev']

    df = df_fusion[max_rev_id].sort_values(by='code') # change / delete the sorting ?

    df.reset_index(drop=True,inplace=True)

    poc = {}

    for i in df['palm oil']:
        if i in poc:
            poc[i] += 1
        else:
            poc[i] = 1
        
    vega = {}

    for i in df['vegan']:
        if i in vega:
            vega[i] += 1
        else:
            vega[i] = 1

    veget = {}

    for i in df['vegetarian']:
        if i in veget:
            veget[i] += 1
        else:
            veget[i] = 1

    count_dict = {}

    count_dict.update(poc)
    count_dict.update(vega)
    count_dict.update(veget)

    df_count = pd.DataFrame.from_dict(count_dict, orient='index', columns = ['Occurrence rate (%)'])

    df_count['Occurrence rate (%)'] = (df_count['Occurrence rate (%)'] / len(df)) * 100

    dummy_df = pd.get_dummies(df[['palm oil', 'vegan', 'vegetarian']], prefix = "", prefix_sep = "")
    reg_df = dummy_df.merge(df['rev'], on = dummy_df.index, how = 'left')
    reg_df.drop('key_0', axis = 1, inplace = True)

    refined_x = reg_df['rev'].apply(lambda x: int(x))

    correl_dict = {}

    for i in dummy_df.columns:
        correl_dict[i] = 0

    for i in correl_dict:
        correl_dict[i] = np.corrcoef(refined_x,reg_df[i])[0,1]

    df_correl = pd.DataFrame.from_dict(correl_dict, orient='index', columns = ['Revision correlation coefficients'])

    final_df = df_count.merge(df_correl, left_index = True, right_index = True)

    final_df.reset_index(inplace = True)

    final_df.rename(columns = {"index" : "Criteria status"}, inplace = True)

    final_df['Criteria status'] = final_df['Criteria status'].apply(lambda x: x[3:])

    if toInsert:
        print("recording in db, chunkSize is", chunkSize)
        for df_chunk in slice_generator(final_df, chunkSize):
              records = df_chunk.to_dict(orient='records')
              pprint(records)
              upserts=[ pymongo.UpdateOne({"_id":x['Criteria status']}, {'$setOnInsert':x}, upsert=True) for x in records]
              result = db.da_IAT.bulk_write(upserts)
              #db.da_users.insert_many(records)
        
    else:
        return final_df


#  ----------------------------------------------
# |                main begins here              |
#  ----------------------------------------------

undefinedValues = ["unknown", ["unknown"]]
requestLimit    = 3000
aggLimit        = 1000

client       = MongoClient(port=27017)
db           = client.off
coll         = db.products_revs

# to handle command line arguments
parser = argparse.ArgumentParser(description='off mongodb client')
parser.add_argument('--user', '-u', type=str, help='user to look at')
parser.add_argument('--id', '-i', type=str, help='id to look at')
parser.add_argument('--insert', '--insert', action='store_true', help='insert data into the db')
parser.add_argument('--all', '-a', action='store_true', help='show undefined fields (in conjonction with other options)')
parser.add_argument('--random', '-r', nargs=1, type=int, help='obtain N random documents given that N is less than 5%% of the total documents in the collection and collection contains more than 100 documents')
parser.add_argument('--userModifier', '--userModifier', action='store_true', help='populates modifyCount based on last_modified_by into da_users')
parser.add_argument('--revsStats', '--revsStats', action='store_true', help='gather stats per revision in da_revs')
parser.add_argument('--collectionUser', '--collectionUser', action='store_true', help='contribution types and source types/names per users')
parser.add_argument('--contributionTypePerProduct', '--contributionTypePerProduct', action='store_true', help='contribution types per products')
parser.add_argument('--contributionTypePerCategory', '--contributionTypePerCategory', action='store_true', help='contribution types per category')
parser.add_argument('--IATStats', '--IATStats', action ='store_true', help ='filling ratios and correlations coefficients with revs for each Ingredients_Analysis_Tags states')
parser.add_argument('--purge', '--purge', action='store_true', help='purge data collections')

args = parser.parse_args()

# flag to insert or not in a new collection, not specifying this flag allows to validate the request before insertion
if args.insert:
  toInsert = True
else:
  toInsert = False

# shows user documents
if args.user:
  showDocsPerUser(args.user)

# shows a specific document
elif args.id:
  showDoc(args.id)

# shows N random documents
elif args.random:
  randomPipeline = [ { "$sample": { "size": args.random[0] } } ]
  pprint(list(coll.aggregate(pipeline)))

elif args.purge:
  purgeCollections()

# ------------- app options

elif args.userModifier:
  if toInsert:
    da_users_userModifier(toInsert)
  else:
    pprint(da_users_userModifier())

elif args.revsStats:
  if toInsert:
    da_revs_stats(toInsert)
  else:
    pprint(da_revs_stats())

elif args.collectionUser:
  if toInsert:
    da_source_contribution_per_user(toInsert)
  else:
    pprint(da_source_contribution_per_user())

elif args.contributionTypePerProduct:
  if toInsert:
    da_types_contribution_per_product(toInsert)
  else:
    pprint(da_types_contribution_per_product())
    
elif args.contributionTypePerCategory:
  if toInsert:
    da_types_contribution_per_category(toInsert)
  else:
    pprint(da_types_contribution_per_category())

elif args.IATStats:
  if toInsert:
    da_IAT_function(toInsert)
  else:
    pprint(da_IAT_function())