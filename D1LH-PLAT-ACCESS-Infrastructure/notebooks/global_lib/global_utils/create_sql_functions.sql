"""
Module: create_sql_functions
Purpose: In this script there are several functions which have the mission to create functions in sql  
Version: 1.0
Author: Dmytro Ilienko
Owner: Dmytro Ilienko
Email: dmytro.ilienko.ext@bayer.com
Dependencies: None
Usage: Every function should be called and pass the correct parameters
Reviewers: None
History:
    Date: 2024-09-03, Version: 1.0, Author: Dmytro Ilienko, Description: Creation of the script
"""

-- Databricks notebook source
CREATE FUNCTION IF NOT EXISTS generaldiscovery_asset_auth_r.V_SP_MAPPING_V1()
  RETURNS STRING
  RETURN
    SELECT COALESCE(MAX(USER), CURRENT_USER()) FROM generaldiscovery_auth_r.sp_mapping_view WHERE SERVICEPRINCIPAL = CURRENT_USER()

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS generaldiscovery_asset_auth_r.V_ACCESS_V1(authContent STRING, fieldName STRING)
  RETURNS TABLE (value string)
  RETURN SELECT `/BIC/DAUTHVLOW` AS VALUE
    FROM generaldiscovery_auth_r.h2r_bic_aclibaautd2_view
    WHERE `/BIC/DAUTHCONT` = V_ACCESS_V1.authContent
    AND FIELDNM = V_ACCESS_V1.fieldName
    AND UPPER(MAIL_ADDR) = UPPER((SELECT generaldiscovery_asset_auth_r.V_SP_MAPPING_V1())) -- use V_SP_MAPPING function to return the mapped user, if one is defined and if not, the current_user()

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS generaldiscovery_asset_auth_r.V_ACCESS_V2(authContent STRING)
  RETURNS TABLE (value string)
  RETURN SELECT `/BIC/DAUTHCONT` AS VALUE
    FROM generaldiscovery_auth_r.h2r_bic_aclibaautd2_view
    WHERE `/BIC/DAUTHCONT` = V_ACCESS_V2.authContent
    AND UPPER(MAIL_ADDR) = UPPER((SELECT generaldiscovery_asset_auth_r.V_SP_MAPPING_V1()))

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS generaldiscovery_asset_auth_r.V_ACCESS_CLMS_POST(authContent STRING DEFAULT CURRENT_USER()) 
  RETURNS TABLE (OID string)
    RETURN select distinct post.OID
    from generaldiscovery_lpc.clms_bydlclms_contracts_post post
    join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_POST.authContent
        and iu.user_id in (post.CREATEUSER_V, post.RESPONSIBLE_OFFICER_V, post.RESPONSIBLE_STEWARD_V, post.RESPONSIBLE_ACCOUNTANT_V, post.RESPONSIBLE_LAWYER_V, post.RESPONSIBLE_SUPERVISOR_V)
    union
    /* Additional participants - users */
    select distinct participants.LINK_SOURCE_OID
    from generaldiscovery_lpc.clms_bydlclms_link_contr_participant participants
    join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_POST.authContent
      and iu.user_id = participants.USERUNIT_V --translate to cwid
    where left(participants.role_v, 4) <> 'PRE_' --only postsigning roles relevant
    union
    /* Additional participants - units */
    select distinct participants.LINK_SOURCE_OID
    from generaldiscovery_lpc.clms_bydlclms_link_contr_participant participants
    inner join generaldiscovery_lpc.clms_bydlclms_user_unit user_unit on user_unit.unit = participants.USERUNIT_V
    join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_POST.authContent and user_unit.cwid = iu.user_id
    where left(participants.role_v, 4) <> 'PRE_' --only postsigning roles relevant

-- COMMAND ----------    

CREATE FUNCTION IF NOT EXISTS generaldiscovery_asset_auth_r.V_ACCESS_CLMS_PRE (authContent STRING DEFAULT CURRENT_USER()) 
  RETURNS TABLE (OID string)
  RETURN select distinct pre.OID
      from generaldiscovery_lpc.clms_bydlclms_contracts_pre pre
      join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_PRE.authContent
        and iu.user_id in (pre.CREATEUSER_V, pre.RESPONSIBLE_OFFICER_V, pre.PRESIGNING_NEGOTIATOR_V, pre.PRESIGNING_SPECIALIST_V, pre.PRESIGNING_REQUESTOR_V)
      union
      /* Direct read access based on presigning specialist group = unit */
      select distinct pre.OID
      from generaldiscovery_lpc.clms_bydlclms_contracts_pre pre
      inner join generaldiscovery_lpc.clms_bydlclms_user_unit user_unit on user_unit.unit = pre.PRESIGNING_SPECIALISTGROUP_V
      join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_PRE.authContent
            and iu.user_id = user_unit.cwid
      union
      /* Additional participants - users */
      select distinct participants.LINK_SOURCE_OID
      from generaldiscovery_lpc.clms_bydlclms_link_contr_participant participants
      join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_PRE.authContent
        and participants.USERUNIT_V = iu.user_id --translate to cwid
      where left(participants.role_v, 4) = 'PRE_' --only presigning roles relevant
      union
      /* Additional participants - units */
      select distinct participants.LINK_SOURCE_OID
      from generaldiscovery_lpc.clms_bydlclms_link_contr_participant participants
      inner join generaldiscovery_lpc.clms_bydlclms_user_unit user_unit on user_unit.unit = participants.USERUNIT_V
      join generaldiscovery_iams.iams_users5 iu on ADUSERPRINCIPALNAME = V_ACCESS_CLMS_PRE.authContent--filter on memberships of the user
          and user_unit.cwid = iu.user_id
      where left(participants.role_v, 4) = 'PRE_' --only presigning roles relevant
