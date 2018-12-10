"""Perform queries of fma data to populate fma-report and timber-sale-report

These functions perform the nessecary database queries and data parsing
to populate the Django templating on the two report pages.
"""

import cx_Oracle

def get_index_form_values(db: str, user: str, password: str) -> tuple:
    '''
    Query ropa to get a list of timbersale names/id's and to FMA names/ids 
    to populate the selection tables of the index page. For timbersales, we only
    want ones that are planned or sold. For FMAs we only want timber harvest 
    activities that are planned. 

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
    '''
    
    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()
    tsnm_tsid_query = """
    SELECT
        TS_ID, TS_NM, TS_STATUS_CD
    FROM
        SHARED_LRM.TS
    WHERE
        TS_STATUS_CD = 'PLANNED' OR TS_STATUS_CD = 'SOLD'
    ORDER BY
        TS_NM
    """
    cursor.execute(tsnm_tsid_query)
    timber_sales = cursor.fetchall()
    fma_query = """
    SELECT
        FMA_NM, FMA_ID 
    FROM
        SHARED_LRM.FMA_V
    WHERE
        SHARED_LRM.FMA_V.FMA_TYPE_CD = 'TIM_HARV' AND SHARED_LRM.FMA_V.FMA_STATUS_CD = 'PLANNED'
    ORDER BY
        SHARED_LRM.FMA_V.FMA_NM
    """
    cursor.execute(fma_query)
    fmas = cursor.fetchall()
    cursor.close()
    connection.close()
    del cursor
    del connection
    return timber_sales, fmas


def get_fma_details(db: str, user: str, password: str, search: str, ts: bool = True ) -> dict:
    """Used by to generate fma detials for the fma-report page. 
    This will get the details of the single fma that the user submitted
    or all of the fmas that make up the timber sale that was submitted.  

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
        search: The search term for the sql query. Will be fma or timber sale name.  
        ts: bool switch so that we can reuse the code for details of
            either timber sale name or fma name. It picks which fma_detail_query
            to use. 
    """
    # Use this query if ts is turned to false, i.e. search by fma id. 
    if not ts:
        fma_detail_query=f"""
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
    SHARED_LRM.FMA_V.ADMIN_NM,
    SHARED_LRM.TS.TS_ID
    FROM
        SHARED_LRM.FMA_HARVEST
        LEFT JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_HARVEST.TS_ID = SHARED_LRM.TS.TS_ID
        RIGHT JOIN SHARED_LRM.FMA_V ON SHARED_LRM.FMA_HARVEST.FMA_ID = SHARED_LRM.FMA_V.FMA_ID
    WHERE
        SHARED_LRM.FMA_V.FMA_ID = :search
        """
    # Use this query if ts is true, i.t. search by timber sale name. 
    if ts:
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
        SHARED_LRM.FMA_V.ADMIN_NM,
        SHARED_LRM.TS.TS_ID
        FROM
            SHARED_LRM.FMA_HARVEST
            RIGHT JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_HARVEST.TS_ID = SHARED_LRM.TS.TS_ID
            LEFT JOIN SHARED_LRM.FMA_V ON SHARED_LRM.FMA_HARVEST.FMA_ID = SHARED_LRM.FMA_V.FMA_ID
        WHERE
            SHARED_LRM.TS.TS_NM = :search
        ORDER BY
            SHARED_LRM.FMA_V.FMA_NM
        """
    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()
    cursor.execute(fma_detail_query, search=search)
    fma_details = {}
    # Deal with the LOB. Oracle text fields are limited to 4000 characters, so the
    # comments field is a Large Data Object that cx_Oracle treats differently. 
    for row in cursor.fetchall():
        if row[10] != None:
            fma_comments = row[10].read()
        else:
            fma_comments = "None"
        fma_details[row[3]] = {}
        fma_details[row[3]]["fma_id"] = row[0]
        fma_details[row[3]]["harvest_comments"] = row[2]
        fma_details[row[3]]["fma_status"] = row[4]
        fma_details[row[3]]["fma_date"] = row[5]
        fma_details[row[3]]["fma_type"] = row[6]
        fma_details[row[3]]["fma_technique"] = row[7]
        fma_details[row[3]]["fma_crew"] = row[8]
        fma_details[row[3]]["fma_acres_treated"] = row[9]
        fma_details[row[3]]["fma_comments"] = fma_comments
        fma_details[row[3]]["fma_region"] = row[11]
        fma_details[row[3]]["fma_district"] = row[12]
        fma_details[row[3]]["fma_admin"] = row[13]
        fma_details[row[3]]["ts_id"] = row[14]
        fma_details[row[3]]["ts_name"] = row[1]
    cursor.close()
    connection.close()
    del cursor
    del connection
    return fma_details

def get_overlapping_fma_details(db: str, user: str, password: str, search: str, ts: bool = True ) -> list:
    """ Used to get the details for all of the fmas that overlap the submitted
    fma or group of fmas in the timber sale. 

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
        search: The search term for the sql query. Will be fma id or timber sale name.  
        ts: bool switch so that we can reuse the code for details of
            either timber sale name or fma name. It picks which fma_detail_query
            to use. 
    """
    # If False, search by fma id
    if not ts:
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
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
        WHERE
            SHARED_LRM.FMA_V.FMA_ID = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        ORDER BY
            SHARED_LRM.FMA_V_1.FMA_DT
        """

    if ts:
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
            SHARED_LRM.TS.TS_NM = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        ORDER BY
            SHARED_LRM.FMA_V_1.FMA_DT
        """
    
    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()
    cursor.execute(fma_overlap_query, search=search)
    # overlapping_fmas = list(row for row in cursor.fetchall())
    overlapping_fmas = []
    for row in cursor.fetchall():
        # The comments field is a LOB object, so we need the read method, which won't work if value is None. 
        if row[8] != None:
            comments = row[8].read()
        else:
            comments = "None"
        overlapping_fmas.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], comments])
    cursor.close()
    connection.close()
    del cursor
    del connection
    return overlapping_fmas

def get_regen_details(db: str, user: str, password: str, search: str, ts: bool = True ) -> list:
    """Used by to get the details of the regen activites from the regen table

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
        search: The search term for the sql query. Will be fma id or timber sale name.  
        ts: bool switch so that we can reuse the code for details of
            either timber sale name or fma name. It picks which regen query. 
    """
    if ts:
        regen_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_REGEN.SPECIES_CD,
            SHARED_LRM.FMA_REGEN.STOCK_TYPE_CD,
            SHARED_LRM.FMA_REGEN.TPA,
            SHARED_LRM.FMA_REGEN.AREA_REGEN,
            SHARED_LRM.FMA_REGEN.STOCK_TOTAL
        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_REGEN  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_REGEN.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.TS.TS_NM = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """
    if not ts:
        regen_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_REGEN.SPECIES_CD,
            SHARED_LRM.FMA_REGEN.STOCK_TYPE_CD,
            SHARED_LRM.FMA_REGEN.TPA,
            SHARED_LRM.FMA_REGEN.AREA_REGEN,
            SHARED_LRM.FMA_REGEN.STOCK_TOTAL
        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_REGEN  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_REGEN.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.FMA_V.FMA_ID = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """
    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()
    cursor.execute(regen_query, search=search)
    regen = [row for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    del cursor
    del connection
    return regen

def get_material_details(db: str, user: str, password: str, search: str, ts: bool = True ) -> list:
    """ Used to get the details of the regen activities that overlap the input
    fma or timber sale. 

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
        search: The search term for the sql query. Will be fma id or timber sale name.  
        ts: bool switch so that we can reuse the code for details of
            either timber sale name or fma name. It picks which fma_detail_query
            to use. 
    """
    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()
    if ts:
        material_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY,
            SHARED_LRM.FMA_MATERIAL.UOM_CD,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY_AC,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_COST,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_COST_AC,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_CD
        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_MATERIAL  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_MATERIAL.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.TS.TS_NM = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """

    if not ts:
        material_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY,
            SHARED_LRM.FMA_MATERIAL.UOM_CD,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY_AC,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_COST,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_COST_AC,
            SHARED_LRM.FMA_MATERIAL.MATERIAL_CD
        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_MATERIAL  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_MATERIAL.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.FMA_V.FMA_ID = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """
    cursor.execute(material_query, search=search)
    material = [row for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    del cursor
    del connection
    return material

def get_survey_details(db: str, user: str, password: str, search: str, ts: bool = True ) -> list:
    """ Used to get the details of the survey activities that overlap the input
    fma or timber sale. 

    Parameters:
        db: the ropa database. 
        user: The ropa username
        password: the ropa password. 
        search: The search term for the sql query. Will be fma id or timber sale name.  
        ts: bool switch so that we can reuse the code for details of
            either timber sale name or fma name. It picks which fma_detail_query
            to use. 
    """

    connection = cx_Oracle.connect(user, password, db)
    cursor = connection.cursor()

    if ts:
        survey_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_SURVEY.SPECIES_CD,
            SHARED_LRM.FMA_SURVEY.TPA_ALL,
            SHARED_LRM.FMA_SURVEY.TPA_PLANTED,
            SHARED_LRM.FMA_SURVEY.TPA_NATURAL,
            SHARED_LRM.FMA_SURVEY.DAMAGE_CD,
            SHARED_LRM.FMA_SURVEY.DBH,
            SHARED_LRM.FMA_SURVEY.CROWN_RATIO,
            SHARED_LRM.FMA_SURVEY.SURVIVAL_PERCENT,
            SHARED_LRM.FMA_SURVEY.HEIGHT

        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_SURVEY  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_SURVEY.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.TS.TS_NM = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """
    if not ts:
        survey_query = f"""
        SELECT
            FMA_V_1.FMA_ID,
            SHARED_LRM.FMA_SURVEY.SPECIES_CD,
            SHARED_LRM.FMA_SURVEY.TPA_ALL,
            SHARED_LRM.FMA_SURVEY.TPA_PLANTED,
            SHARED_LRM.FMA_SURVEY.TPA_NATURAL,
            SHARED_LRM.FMA_SURVEY.DAMAGE_CD,
            SHARED_LRM.FMA_SURVEY.DBH,
            SHARED_LRM.FMA_SURVEY.CROWN_RATIO,
            SHARED_LRM.FMA_SURVEY.SURVIVAL_PERCENT,
            SHARED_LRM.FMA_SURVEY.HEIGHT

        FROM
            SHARED_LRM.FMA_V
            INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
            INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
            INNER JOIN SHARED_LRM.FMA_SURVEY  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_SURVEY.FMA_ID
            INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
        WHERE
            SHARED_LRM.FMA_V.FMA_ID = :search AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.25
        """
    cursor.execute(survey_query, search=search)
    survey = [row for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    del cursor
    del connection
    return survey