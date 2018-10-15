from django.shortcuts import render, get_object_or_404
from collections import OrderedDict

# Oracle database connection configuration
ORACLE_DATABASE = 'ropa'
ORACLE_USERNAME = 'gis_layer_user'
ORACLE_PASSWORD = 'gis_layer_user_ropa'

def index(request):
    ORACLE_DATABASE = 'ropa'
    ORACLE_USERNAME = 'gis_layer_user'
    ORACLE_PASSWORD = 'gis_layer_user_ropa'
    import cx_Oracle
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_DATABASE)
    cursor = connection.cursor()
    cursor.execute("select TS_ID, TS_NM from SHARED_LRM.TS order by TS_NM")
    timber_sales = cursor.fetchall()
    cursor.close()
    connection.close()
    return render(request, 'report/index.html', {'timber_sales' : timber_sales})

def fma_report(request):
    import cx_Oracle
    ORACLE_DATABASE = 'ropa'
    ORACLE_USERNAME = 'gis_layer_user'
    ORACLE_PASSWORD = 'gis_layer_user_ropa'
    timber_sale=request.GET.get('timber-sale')
    # Create a list of FMAs that overlap the timber harvest FMAs
    fma_overlap_query = f"""
    SELECT
      SHARED_LRM.FMA_V.FMA_ID,
      SHARED_LRM.FMA_V_1.FMA_ID,
      FMA_V_1.FMA_NM,
      FMA_V_1.FMA_TYPE_CD,
      FMA_V_1.TECHNIQUE_CD,
      FMA_V_1.FMA_DT,
      FMA_V_1.FMA_STATUS_CD,
      SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP,
      FMA_V_1.COMMENTS
    FROM
      SHARED_LRM.TS
      INNER JOIN SHARED_LRM.FMA_V
      INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
      INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
      ON SHARED_LRM.TS.TS_ID = SHARED_LRM.FMA_V.TS_ID
    WHERE
      SHARED_LRM.TS.TS_NM = '{timber_sale}' AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.1
    ORDER BY
      SHARED_LRM.FMA_V_1.FMA_DT
    """
    
    connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_DATABASE)
    cursor = connection.cursor()
    cursor.execute(fma_overlap_query)
    #overlapping_fmas = list(row for row in cursor.fetchall())
    overlapping_fmas = []
    for row in cursor.fetchall():
      # The comments field is a LOB object, so we need the read method, which won't work if value is None. 
      if row[8] != None:
        comments = row[8].read()
      else:
        comments = "None"
      
      overlapping_fmas.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], comments])
    # Get the details for each fma in the timber sale
    fma_detail_query = f"""
    SELECT
    SHARED_LRM.FMA_HARVEST.FMA_ID,
    SHARED_LRM.TS.TS_NM,
    SHARED_LRM.FMA_HARVEST.COMMENTS,
    SHARED_LRM.FMA_V.FMA_NM,
    SHARED_LRM.FMA_V.FMA_STATUS_CD,
    SHARED_LRM.FMA_V.FMA_DT,
    SHARED_LRM.FMA_V.FMA_TYPE_CD,
    SHARED_LRM.FMA_V.TECHNIQUE_CD,
    SHARED_LRM.FMA_V.CREW_CD,
    SHARED_LRM.FMA_V.ACRES_TREATED,
    SHARED_LRM.FMA_V.COMMENTS,
    SHARED_LRM.FMA_V.REGION_NM,
    SHARED_LRM.FMA_V.DISTRICT_NM,
    SHARED_LRM.FMA_V.ADMIN_NM
    FROM
        SHARED_LRM.FMA_HARVEST
        RIGHT JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_HARVEST.TS_ID = SHARED_LRM.TS.TS_ID
        LEFT JOIN SHARED_LRM.FMA_V ON SHARED_LRM.FMA_HARVEST.FMA_ID = SHARED_LRM.FMA_V.FMA_ID
    WHERE
      SHARED_LRM.TS.TS_NM = '{timber_sale}'
    ORDER BY
      SHARED_LRM.FMA_V.FMA_NM
    """
    cursor.execute(fma_detail_query)
    fma_details = {}
    for row in cursor.fetchall():
      fma_details[row[3]] = {}
      fma_details[row[3]]["fma_id"] = row[0]
      fma_details[row[3]]["harvest_comments"] = row[2]
      fma_details[row[3]]["fma_status"] = row[4]
      fma_details[row[3]]["fma_date"] = row[5]
      fma_details[row[3]]["fma_type"] = row[6]
      fma_details[row[3]]["fma_technique"] = row[7]
      fma_details[row[3]]["fma_crew"] = row[8]
      fma_details[row[3]]["fma_acres_treated"] = row[9]
      fma_details[row[3]]["fma_comments"] = row[10]
      fma_details[row[3]]["fma_region"] = row[11]
      fma_details[row[3]]["fma_district"] = row[12]
      fma_details[row[3]]["fma_admin"] = row[13]
    cursor.close()
    connection.close()
    del cursor
    del connection

    return render(request, 'report/fma-report.html', {'overlapping_fmas' : overlapping_fmas, 'fma_details' : fma_details, 'timber_sale' : timber_sale})

