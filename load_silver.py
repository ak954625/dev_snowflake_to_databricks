# Databricks notebook source
qry= """

WITH EXP AS
(
  SELECT DISTINCT SC_EXP_PO_NO FROM logistics.bronze_VW_SC_EXP_ORDER
),

TPO AS
(
SELECT TC_PURCHASE_ORDERS_ID, 
       MAX(D_FACILITY_ALIAS_ID) AS D_FACILITY_ALIAS_ID, 
       MAX(BUSINESS_PARTNER_ID) AS BUSINESS_PARTNER_ID, 
       MAX(D_CITY) AS D_CITY, 
       any_value(D_COUNTRY_CODE) AS D_COUNTRY_CODE, 
       any_value(O_COUNTRY_CODE) AS O_COUNTRY_CODE, 
       MAX(BILLING_METHOD) AS BILLING_METHOD, 
       MAX(O_CITY) AS O_CITY, 
       MAX(O_STATE_PROV) AS O_STATE_PROV, 
       MAX(PURCHASE_ORDERS_ID) AS PURCHASE_ORDERS_ID,
       MAX(LAST_UPDATED_DTTM) AS LAST_UPDATED_DTTM,
       MAX(CREATED_DTTM) AS CREATED_DTTM,
CASE WHEN any_value(O_COUNTRY_CODE) != any_value(D_COUNTRY_CODE) AND any_value(O_COUNTRY_CODE) != 'CA' THEN 'INTERNATIONAL'
     WHEN SUBSTRING(CAST(MAX(O_FACILITY_ALIAS_ID) AS STRING),0,2) = '57' AND MAX(EXP.SC_EXP_PO_NO) IS NOT NULL THEN 'INTERNATIONAL'
     WHEN MAX(BILLING_METHOD) = 0 THEN 'PREPAID'
     WHEN MAX(BILLING_METHOD) = 1 THEN 'COLLECT' END AS PO_TYPE
FROM logistics.bronze_VW_SC_TLM_PURCHASE_ORDERS TPO
LEFT JOIN EXP ON EXP.SC_EXP_PO_NO = TPO.TC_PURCHASE_ORDERS_ID
WHERE TPO.CREATED_SOURCE = 'SAP'
GROUP BY TC_PURCHASE_ORDERS_ID
),

PPOL AS 
(
     SELECT DISTINCT PO_NO FROM logistics.bronze_vw_pending_po_line
)

,POL AS (
	SELECT PO_NO
		,PO_TRACKING_NO
		,MAX(DELIV_DATE) AS DELIV_DATE
		,MAX(SHIP_START_DATE) AS SHIP_START_DATE
		,MAX(SHIP_END_DATE) AS SHIP_END_DATE
		,MAX(DELIV_START_DATE) AS DELIV_START_DATE
		,MAX(DELIV_END_DATE) AS DELIV_END_DATE
		,SUM(PO_LINE_UNIT_PRICE * PO_LINE_ORDER_QTY) AS MONETARY_VALUE
		,SUM(PO_LINE_ORDER_QTY) AS ORDER_QTY
		,SUM(PO_LINE_RCVD_QTY) AS RCVD_QTY
		,MAX(PO_TOTAL_WEIGHT) AS PO_WEIGHT
		,MAX(PO_TOTAL_VOLUME) AS PO_VOLUME
		,FREIGHT_TYPE_CODE
		,PO_LINE_MOVE_TYPE_CODE
		,MAX(po_line_max_rcvd_date) AS PO_RCVD_DATE
          ,IFF(SUM(CASE 
					WHEN logistics.bronze_VW_purchase_order_line.po_line_max_rcvd_date IS NULL
						THEN 1
					ELSE 0
					END) > 0, NULL, MAX(logistics.bronze_VW_purchase_order_line.po_line_max_rcvd_date)) AS PO_FULL_RCVD_DATE
	FROM logistics.bronze_VW_PURCHASE_ORDER_LINE
	GROUP BY PO_NO
		,PO_TRACKING_NO
		,FREIGHT_TYPE_CODE
		,PO_LINE_MOVE_TYPE_CODE
	),
CTS AS (
	SELECT EXP.PO
		,CTSH.SC_CT_SHIPMENT_ID
		,MAX(EXP.VENDOR_BOOKED_DATE) AS VENDOR_BOOKED_DATE
		,MAX(EXP.CARGO_READY_DATE) AS CARGO_READY_DATE
		,MAX(CTSH.SC_CT_SHIPMENT_START_DATE) AS SC_CT_SHIPMENT_START_DATE
		,MAX(SHIP_NOT_AFTER) AS SHIP_NOT_AFTER
		,MAX(SHIP_NOT_BEFORE) AS SHIP_NOT_BEFORE
	FROM logistics.bronze_VW_SC_CT_SHIPMENT CTSH
	LEFT JOIN (
		SELECT SC_EXP_PO_NO as PO
			,ARTICLE_NO as SKU
			,MAX(SC_EXP_SHIP_NOT_AFTER_DATE) AS SHIP_NOT_AFTER
			,MAX(SC_EXP_SHIP_NOT_BEFORE_DATE) AS SHIP_NOT_BEFORE
			,MIN(SC_EXP_VENDOR_BOOKED_DATE) AS VENDOR_BOOKED_DATE
			,MAX(SC_EXP_CARGO_READY_DATE) AS CARGO_READY_DATE
		FROM logistics.bronze_VW_SC_EXP_ORDER
		GROUP BY PO, SKU
		) EXP ON CTSH.TSC_PO_NO = EXP.PO
		AND CTSH.ARTICLE_NO = EXP.SKU
	GROUP BY EXP.PO, CTSH.SC_CT_SHIPMENT_ID
	)

	,
CTSE AS (
	SELECT SC_CT_SHIPMENT_ID AS CT_SHIPMENT_ID
		,MAX(CASE 
				WHEN SC_CT_SHIPMENT_EVENT_CODE = 'COB'
					THEN SC_CT_SHIPMENT_EVENT_TS
				ELSE NULL
				END) AS COB_TS
		,MAX(CASE 
				WHEN SC_CT_SHIPMENT_EVENT_CODE = 'CARRDROP'
					OR SC_CT_SHIPMENT_EVENT_CODE = 'DROP'
					THEN SC_CT_SHIPMENT_EVENT_TS
				ELSE NULL
				END) AS CARRIER_DROP_TS
	FROM logistics.bronze_VW_SC_CT_SHIPMENT_EVENT
	GROUP BY SC_CT_SHIPMENT_ID
	)
	,
TSH AS (
	SELECT SHIPMENT_ID
		,MAX(SHIPMENT_STATUS) AS SHIPMENT_STATUS
		,MAX(STATUS_CHANGE_DTTM) AS STATUS_CHANGE_DTTM
		,MAX(TRAILER_NUMBER) AS TRAILER_NUMBER
	FROM logistics.bronze_VW_SC_TLM_SHIPMENT
	GROUP BY SHIPMENT_ID
	)
,
RLI AS (
	SELECT ORDER_ID
		,MAX(PICKUP_END_DTTM) AS PICKUP_END_DTTM
	FROM logistics.bronze_VW_SC_TLM_RTS_LINE_ITEM
	GROUP BY ORDER_ID
	),

ORD AS
(
  SELECT TC_MASTER_ORDER_ID, MAX(O_FACILITY_ALIAS_ID) AS O_FACILITY_ALIAS_ID, MAX(SHIPMENT_ID) AS SHIPMENT_ID, MAX(TC_SHIPMENT_ID) AS TC_SHIPMENT_ID, MAX(ORDER_ID) AS ORDER_ID, MAX(CREATED_DTTM) AS CREATED_DTTM FROM
  (
  SELECT OMO.TC_MASTER_ORDER_ID, O.* 
  FROM logistics.bronze_VW_SC_TLM_ORDER_MASTER_ORDER OMO
  LEFT JOIN logistics.bronze_VW_SC_TLM_ORDERS O ON OMO.ORDER_ID = O.ORDER_ID
  )
  GROUP BY TC_MASTER_ORDER_ID
),

A AS (
	SELECT TPO.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID
    ,TPO.PO_TYPE
		,ORD.O_FACILITY_ALIAS_ID AS O_FACILITY_ALIAS_ID
		,TPO.D_FACILITY_ALIAS_ID AS D_FACILITY_ALIAS_ID
		,ORD.SHIPMENT_ID AS SHIPMENT_ID
		,ORD.TC_SHIPMENT_ID AS TC_SHIPMENT_ID
		,TPO.BUSINESS_PARTNER_ID AS BUSINESS_PARTNER_ID
		,POL.PO_TRACKING_NO
          ,TPO.CREATED_DTTM AS PO_DATE
		,TPO.D_CITY
		,TPO.D_COUNTRY_CODE
		,TPO.O_COUNTRY_CODE
		,TPO.BILLING_METHOD
		,POL.PO_RCVD_DATE
		,ORD.SHIPMENT_ID AS SHIPMENT_ID_O
		,TSH.SHIPMENT_STATUS
		,TSH.STATUS_CHANGE_DTTM
		,ORD.ORDER_ID
		,ORD.CREATED_DTTM
		,POL.SHIP_END_DATE
		,POL.DELIV_DATE
		,POL.DELIV_START_DATE
		,POL.DELIV_END_DATE
		,POL.SHIP_START_DATE
		,TPO.O_CITY
		,POL.PO_WEIGHT
		,PO_VOLUME
		,POL.ORDER_QTY
		,POL.MONETARY_VALUE
		,TPO.O_STATE_PROV
		,TSH.TRAILER_NUMBER
		,POL.FREIGHT_TYPE_CODE
		,POL.PO_LINE_MOVE_TYPE_CODE
		,RLI.PICKUP_END_DTTM
          ,POL.PO_NO
          ,POL.PO_FULL_RCVD_DATE
          ,TPO.LAST_UPDATED_DTTM
	FROM TPO
	LEFT JOIN POL ON TPO.TC_PURCHASE_ORDERS_ID = POL.PO_NO
	LEFT JOIN ORD ON TPO.TC_PURCHASE_ORDERS_ID = ORD.TC_MASTER_ORDER_ID
	LEFT JOIN TSH ON ORD.SHIPMENT_ID = TSH.SHIPMENT_ID
	LEFT JOIN RLI ON TPO.PURCHASE_ORDERS_ID = RLI.ORDER_ID
	),
 
TST AS
(
     SELECT SHIPMENT_ID, FACILITY_ALIAS_ID, STOP_SEQ, MAX(DEPARTURE_START_DTTM) AS DEPARTURE_START_DTTM, MAX(ARRIVAL_START_DTTM) AS ARRIVAL_START_DTTM FROM logistics.bronze_VW_SC_TLM_STOP GROUP BY SHIPMENT_ID, FACILITY_ALIAS_ID, STOP_SEQ
),
 
B AS (
	SELECT A.*
		,MAX(CASE 
				WHEN TST.FACILITY_ALIAS_ID = A.O_FACILITY_ALIAS_ID
					THEN TST.DEPARTURE_START_DTTM
				END) AS ORIGIN_DEPARTURE_DTTM
		,MAX(CASE 
				WHEN TST.FACILITY_ALIAS_ID = A.D_FACILITY_ALIAS_ID
					THEN TST.ARRIVAL_START_DTTM
				END) AS DESTINATION_ARRIVAL_DTTM
		,MAX(CASE 
				WHEN TST.FACILITY_ALIAS_ID = A.O_FACILITY_ALIAS_ID
					THEN TST.STOP_SEQ
				END) AS ORIGIN_STOP
                    ,MAX(CASE WHEN TST.FACILITY_ALIAS_ID = A.D_FACILITY_ALIAS_ID THEN TST.STOP_SEQ END) AS DEST_STOP
	FROM A
	LEFT JOIN TST ON TST.SHIPMENT_ID = A.SHIPMENT_ID
	GROUP BY A.TC_PURCHASE_ORDERS_ID
    ,A.PO_TYPE
    ,A.PO_DATE
		,A.O_FACILITY_ALIAS_ID
		,A.D_FACILITY_ALIAS_ID
		,A.SHIPMENT_ID
		,A.TC_SHIPMENT_ID
		,A.BUSINESS_PARTNER_ID
		,A.PO_TRACKING_NO
		,A.D_CITY
		,A.D_COUNTRY_CODE
		,A.O_COUNTRY_CODE
		,A.BILLING_METHOD
		,A.PO_RCVD_DATE
		,A.SHIPMENT_ID_O
		,A.SHIPMENT_STATUS
		,A.STATUS_CHANGE_DTTM
		,A.ORDER_ID
		,A.CREATED_DTTM
		,A.SHIP_END_DATE
		,A.DELIV_DATE
		,A.DELIV_START_DATE
		,A.DELIV_END_DATE
		,A.SHIP_START_DATE
		,A.O_CITY
		,A.PO_WEIGHT
		,A.PO_VOLUME
		,A.ORDER_QTY
		,A.MONETARY_VALUE
		,A.O_STATE_PROV
		,A.TRAILER_NUMBER
		,A.FREIGHT_TYPE_CODE
		,A.PO_LINE_MOVE_TYPE_CODE
		,A.PICKUP_END_DTTM
          ,A.PO_NO
          ,A.PO_FULL_RCVD_DATE
          ,LAST_UPDATED_DTTM
	)
	,
TTM AS (
	SELECT SHIPMENT_ID
		,STOP_SEQ
		,MESSAGE_STATUS
		,MESSAGE_TYPE
		,MAX(EVENT_DTTM) AS EVENT_DTTM
	FROM logistics.bronze_VW_SC_TLM_TRACKING_MESSAGE
	GROUP BY SHIPMENT_ID
		,STOP_SEQ
		,MESSAGE_STATUS
		,MESSAGE_TYPE
	),
C AS (
	SELECT B.*
		,TTM.EVENT_DTTM AS ACTUAL_ORIGIN_DEPARTURE_DTTM
          ,TTM2.EVENT_DTTM AS ACTUAL_ON_YARD_DTTM
	FROM B
	LEFT JOIN TTM ON B.SHIPMENT_ID = TTM.SHIPMENT_ID AND TTM.STOP_SEQ = B.ORIGIN_STOP AND TTM.MESSAGE_TYPE = 50 AND TTM.MESSAGE_STATUS = 10
     LEFT JOIN TTM TTM2 ON B.SHIPMENT_ID = TTM2.SHIPMENT_ID AND TTM2.STOP_SEQ = B.DEST_STOP AND TTM2.MESSAGE_TYPE = 45 AND TTM2.MESSAGE_STATUS = 10      
	),

SE AS 
(
  SELECT SHIPMENT_ID, FIELD_NAME, NEW_VALUE, MAX(CREATED_DTTM) AS PLANNED_TIME FROM logistics.bronze_vw_sc_tlm_shipment_event WHERE FIELD_NAME = 'STATUS' AND NEW_VALUE = 'Assigned'  GROUP BY SHIPMENT_ID, FIELD_NAME, NEW_VALUE
),

M AS (
	SELECT 
      C.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
      C.PO_DATE AS PO_DATE,
      C.PO_TYPE AS PO_TYPE,
      C.ACTUAL_ON_YARD_DTTM AS ACTUAL_ON_YARD_DTTM,
      WASN.ACTUAL_SHIPPED_DTTM AS ACTUAL_SHIPPED_DTTM,
      CTS.SC_CT_SHIPMENT_START_DATE AS SC_CT_SHIPMENT_START_DATE,
      CTS.SHIP_NOT_AFTER,
      CTS.SHIP_NOT_BEFORE,
      CTS.VENDOR_BOOKED_DATE,
      CTS.CARGO_READY_DATE,
      CTS.SC_CT_SHIPMENT_ID,
      CTSE.CARRIER_DROP_TS AS CARRIER_DROP_TS,
      CTSE.COB_TS AS COB_TS,
      C.CREATED_DTTM AS CREATED_DTTM,
      C.SHIP_END_DATE AS SHIP_END_DATE,
      C.TC_SHIPMENT_ID AS TC_SHIPMENT_ID,
      C.SHIPMENT_STATUS AS SHIPMENT_STATUS,
      C.SHIP_START_DATE AS SHIP_START_DATE,
      C.ACTUAL_ORIGIN_DEPARTURE_DTTM AS ACTUAL_ORIGIN_DEPARTURE_DTTM,
      C.DESTINATION_ARRIVAL_DTTM AS DESTINATION_ARRIVAL_DTTM,
      C.DELIV_DATE AS DELIV_DATE,
      C.ORIGIN_DEPARTURE_DTTM as ORIGIN_DEPARTURE_DTTM,
      C.DELIV_END_DATE AS DELIV_END_DATE,
      C.PO_RCVD_DATE AS PO_RCVD_DATE,
      C.DELIV_START_DATE,
      C.ORDER_ID,
      C.SHIPMENT_ID_O,
      NULL AS COLLECT_TS,
      C.PICKUP_END_DTTM,
      C.STATUS_CHANGE_DTTM,
      PPOL.PO_NO AS P_PO_NO,
      C.PO_NO AS PO_NO,
      C.PO_FULL_RCVD_DATE,
      C.LAST_UPDATED_DTTM,
      SE.PLANNED_TIME
	FROM C
	LEFT JOIN logistics.bronze_VW_SC_WMS_ASN WASN ON C.TC_SHIPMENT_ID = WASN.TC_SHIPMENT_ID
	LEFT JOIN CTS ON C.TC_PURCHASE_ORDERS_ID = CTS.PO
	LEFT JOIN CTSE ON CTS.SC_CT_SHIPMENT_ID = CTSE.CT_SHIPMENT_ID
  LEFT JOIN PPOL ON PPOL.PO_NO = C.TC_PURCHASE_ORDERS_ID
  LEFT JOIN SE ON C.SHIPMENT_ID = SE.SHIPMENT_ID
  WHERE NOT (PO_TYPE = 'INTERNATIONAL' AND SC_CT_SHIPMENT_ID IS NULL)
	),

CREATED_POS AS 
(
         SELECT * FROM 
    (
          SELECT 
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,    
          NULL AS EXPECTED_ETA_FOR_MILESTONE,
          M.PO_DATE AS ACTUAL_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.VENDOR_BOOKED_G), M.SHIP_NOT_AFTER) 
               WHEN M.PO_TYPE = 'COLLECT' THEN DATEADD(HOUR, -(CT.MARKED_AS_READY_G), M.SHIP_END_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, -(CT.IN_TRANSIT_G), M.DELIV_START_DATE) END AS NEXT_MILESTONE_EXPECTED_ETA,
          NULL AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN M.DELIV_DATE
               WHEN M.PO_TYPE = 'PREPAID' THEN M.DELIV_END_DATE END AS REVISED_ETA_FOR_PO,
          1 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND M.TC_PURCHASE_ORDERS_ID IS NOT NULL THEN 'CREATED'
               WHEN M.PO_TYPE = 'PREPAID' AND M.TC_PURCHASE_ORDERS_ID IS NOT NULL THEN 'CREATED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.TC_PURCHASE_ORDERS_ID IS NOT NULL THEN 'CREATED' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'CREATED' THEN 'GREEN'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'CREATED' THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'CREATED' THEN 'GREEN' END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
     ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
)     ,

VENDOR_BOOKED_POS as
(
         SELECT * FROM (
         SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          DATEADD(DAY, -(CT.VENDOR_BOOKED_G), M.SHIP_NOT_AFTER) AS EXPECTED_ETA_FOR_MILESTONE,
          M.VENDOR_BOOKED_DATE AS ACTUAL_ETA_FOR_MILESTONE,
          DATEADD(DAY, -(CT.MARKED_AS_READY_G), M.SHIP_NOT_AFTER) AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          2 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.VENDOR_BOOKED_DATE IS NOT NULL THEN 'VENDOR_BOOKED' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'VENDOR_BOOKED' AND DATEDIFF(DAY, M.VENDOR_BOOKED_DATE, M.SHIP_NOT_AFTER) >= CT.VENDOR_BOOKED_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'VENDOR_BOOKED' AND DATEDIFF(DAY, M.VENDOR_BOOKED_DATE, M.SHIP_NOT_AFTER) >= CT.VENDOR_BOOKED_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'VENDOR_BOOKED' AND DATEDIFF(DAY, M.VENDOR_BOOKED_DATE, M.SHIP_NOT_AFTER) < CT.VENDOR_BOOKED_R THEN 'RED' END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
          ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
),
MARKED_AS_READY_POS AS (
     SELECT * FROM (
         SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          CASE WHEN M.PO_TYPE = 'COLLECT' THEN DATEADD(HOUR, -(CT.MARKED_AS_READY_G), M.SHIP_END_DATE)
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.MARKED_AS_READY_G), M.SHIP_NOT_AFTER) END AS EXPECTED_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' THEN M.CREATED_DTTM
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.CARGO_READY_DATE END AS ACTUAL_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' THEN DATEADD(HOUR, -(CT.PLANNED_G), M.PICKUP_END_DTTM)
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.PLANNED_G), M.SHIP_NOT_AFTER) END AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          3 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND M.ORDER_ID IS NOT NULL THEN 'MARKED_AS_READY'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.CARGO_READY_DATE IS NOT NULL THEN 'MARKED_AS_READY' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'MARKED_AS_READY' AND DATEDIFF(HOUR, M.CREATED_DTTM, M.SHIP_END_DATE) > CT.MARKED_AS_READY_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'MARKED_AS_READY' AND DATEDIFF(HOUR, M.CREATED_DTTM, M.SHIP_END_DATE) >= CT.MARKED_AS_READY_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'MARKED_AS_READY' AND DATEDIFF(HOUR, M.CREATED_DTTM, M.SHIP_END_DATE) < CT.MARKED_AS_READY_R THEN 'RED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'MARKED_AS_READY' AND DATEDIFF(DAY, M.CARGO_READY_DATE, M.SHIP_NOT_AFTER) >= CT.MARKED_AS_READY_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'MARKED_AS_READY' AND DATEDIFF(DAY, M.CARGO_READY_DATE, M.SHIP_NOT_AFTER) < CT.MARKED_AS_READY_R THEN 'RED' END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
          ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
),
PLANNED_POS as (
     SELECT * FROM (
         SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          CASE WHEN M.PO_TYPE = 'COLLECT' THEN  DATEADD(HOUR, -(CT.PLANNED_G), M.PICKUP_END_DTTM)
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.PLANNED_G), M.SHIP_NOT_AFTER) END AS EXPECTED_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT'  THEN M.PLANNED_TIME
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_START_DATE END AS ACTUAL_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' THEN DATEADD(HOUR, -(DATEDIFF(HOUR, M.ORIGIN_DEPARTURE_DTTM, M.DESTINATION_ARRIVAL_DTTM) + CT.IN_TRANSIT_G), M.DELIV_DATE)
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.IN_TRANSIT_G), M.SHIP_NOT_AFTER) END AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          4 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND M.SHIPMENT_ID_O IS NOT NULL AND M.SHIPMENT_STATUS IN (60, 70, 80, 90) THEN 'PLANNED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.SC_CT_SHIPMENT_START_DATE IS NOT NULL THEN 'PLANNED' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'PLANNED' AND ACTUAL_ETA_FOR_MILESTONE < EXPECTED_ETA_FOR_MILESTONE THEN 'GREEN'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'PLANNED' AND ACTUAL_ETA_FOR_MILESTONE <= DATEADD(HOUR, CT.PLANNED_A,M.PICKUP_END_DTTM) THEN 'AMBER'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'PLANNED' AND ACTUAL_ETA_FOR_MILESTONE > M.PICKUP_END_DTTM THEN 'RED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'PLANNED' AND DATEDIFF(DAY, M.SC_CT_SHIPMENT_START_DATE, M.SHIP_NOT_AFTER) > CT.PLANNED_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'PLANNED' AND DATEDIFF(DAY, M.SC_CT_SHIPMENT_START_DATE, M.SHIP_NOT_AFTER) >= CT.PLANNED_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'PLANNED' AND DATEDIFF(DAY, M.SC_CT_SHIPMENT_START_DATE, M.SHIP_NOT_AFTER) < CT.PLANNED_R THEN 'RED' END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
        ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
),

IN_TRANSIT_POS as 
(
     SELECT * FROM (
          SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          CASE WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, -(CT.IN_TRANSIT_G), M.DELIV_START_DATE)
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(DAY, -(CT.IN_TRANSIT_G), M.SHIP_NOT_AFTER)
               WHEN M.PO_TYPE = 'COLLECT' THEN DATEADD(HOUR, -(DATEDIFF(HOUR, M.ORIGIN_DEPARTURE_DTTM, M.DESTINATION_ARRIVAL_DTTM) + CT.IN_TRANSIT_G), M.DELIV_DATE) END AS EXPECTED_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'PREPAID' THEN M.ACTUAL_SHIPPED_DTTM 
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.COB_TS 
               WHEN M.PO_TYPE = 'COLLECT' THEN M.ACTUAL_ORIGIN_DEPARTURE_DTTM END AS ACTUAL_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(HOUR, -(CT.ON_YARD_G), M.DELIV_DATE) 
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, -(CT.ON_YARD_G), M.DELIV_END_DATE) END AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          5 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND M.ACTUAL_ORIGIN_DEPARTURE_DTTM IS NOT NULL THEN 'IN_TRANSIT'
               WHEN M.PO_TYPE = 'PREPAID' AND M.ACTUAL_SHIPPED_DTTM IS NOT NULL THEN 'IN_TRANSIT'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.COB_TS IS NOT NULL THEN 'IN_TRANSIT' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'IN_TRANSIT' AND ACTUAL_ETA_FOR_MILESTONE < EXPECTED_ETA_FOR_MILESTONE THEN 'GREEN'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'IN_TRANSIT' AND ACTUAL_ETA_FOR_MILESTONE = EXPECTED_ETA_FOR_MILESTONE THEN 'AMBER'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'IN_TRANSIT' AND ACTUAL_ETA_FOR_MILESTONE > EXPECTED_ETA_FOR_MILESTONE THEN 'RED'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.ACTUAL_SHIPPED_DTTM, M.DELIV_START_DATE) > CT.IN_TRANSIT_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.ACTUAL_SHIPPED_DTTM, M.DELIV_START_DATE) = CT.IN_TRANSIT_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.ACTUAL_SHIPPED_DTTM, M.DELIV_START_DATE) < CT.IN_TRANSIT_R THEN 'RED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.COB_TS, M.SHIP_NOT_AFTER) > CT.IN_TRANSIT_G THEN 'GREEN' 
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.COB_TS, M.SHIP_NOT_AFTER) >= CT.IN_TRANSIT_A THEN 'AMBER' 
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'IN_TRANSIT' AND DATEDIFF(DAY, M.COB_TS, M.SHIP_NOT_AFTER) < CT.IN_TRANSIT_R THEN 'RED' END AS ADHERENCE_STATUS
          FROM M
               LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
          ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
),

ON_YARD_POS as 
(
     SELECT * FROM (
         SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          CASE WHEN M.PO_TYPE = 'COLLECT' OR M.PO_TYPE = 'INTERNATIONAL' THEN DATEADD(HOUR, -(CT.ON_YARD_G), M.DELIV_DATE) 
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, -(CT.ON_YARD_G), M.DELIV_END_DATE) END AS EXPECTED_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' OR M.PO_TYPE = 'PREPAID' THEN M.ACTUAL_ON_YARD_DTTM
               WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.CARRIER_DROP_TS END AS ACTUAL_ETA_FOR_MILESTONE,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN M.DELIV_DATE
               WHEN M.PO_TYPE = 'PREPAID' THEN M.DELIV_END_DATE END AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          6 AS MILESTONE_RANK,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND M.ACTUAL_ON_YARD_DTTM IS NOT NULL THEN 'ON_YARD'
               WHEN M.PO_TYPE = 'PREPAID' AND M.ACTUAL_ON_YARD_DTTM IS NOT NULL THEN 'ON_YARD'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND M.CARRIER_DROP_TS IS NOT NULL THEN 'ON_YARD' END AS MILESTONE,
          CASE WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, TO_TIMESTAMP(M.ACTUAL_ON_YARD_DTTM), TO_TIMESTAMP(M.DELIV_DATE)) >= CT.ON_YARD_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, TO_TIMESTAMP(M.ACTUAL_ON_YARD_DTTM), TO_TIMESTAMP(M.DELIV_DATE)) >= CT.ON_YARD_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, TO_TIMESTAMP(M.ACTUAL_ON_YARD_DTTM), TO_TIMESTAMP(M.DELIV_DATE)) < CT.ON_YARD_R THEN 'RED'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'ON_YARD' AND DATEDIFF(DAY, M.ACTUAL_ON_YARD_DTTM, M.DELIV_END_DATE) >= CT.ON_YARD_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'ON_YARD' AND DATEDIFF(DAY, M.ACTUAL_ON_YARD_DTTM, M.DELIV_END_DATE) < CT.ON_YARD_R THEN 'RED' 
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, M.CARRIER_DROP_TS, M.DELIV_DATE) >= CT.ON_YARD_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, M.CARRIER_DROP_TS, M.DELIV_DATE) > CT.ON_YARD_A THEN 'AMBER'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'ON_YARD' AND DATEDIFF(HOUR, M.CARRIER_DROP_TS, M.DELIV_DATE) < CT.ON_YARD_R THEN 'RED' END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
          ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
)
,RECEIVED_POS as 
(
     SELECT * FROM(
         SELECT
          M.TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
          CASE WHEN M.PO_TYPE = 'INTERNATIONAL' THEN M.SC_CT_SHIPMENT_ID
               ELSE M.TC_SHIPMENT_ID END AS TC_SHIPMENT_ID,
          M.PO_TYPE,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN M.DELIV_DATE
               WHEN M.PO_TYPE = 'PREPAID' THEN M.DELIV_END_DATE END AS EXPECTED_ETA_FOR_MILESTONE,
          M.PO_RCVD_DATE AS ACTUAL_ETA_FOR_MILESTONE,
          NULL AS NEXT_MILESTONE_EXPECTED_ETA,
          CASE WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) > 0 THEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE)
               WHEN DATEDIFF(DAY, EXPECTED_ETA_FOR_MILESTONE, ACTUAL_ETA_FOR_MILESTONE) <= 0 THEN 0 END AS CURRENT_DELAY_IN_DAYS,
          CASE WHEN M.PO_TYPE IN ('COLLECT', 'INTERNATIONAL') THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_DATE)
               WHEN M.PO_TYPE = 'PREPAID' THEN DATEADD(DAY, CURRENT_DELAY_IN_DAYS, M.DELIV_END_DATE) END AS REVISED_ETA_FOR_PO,
          7 AS MILESTONE_RANK,
          CASE 
               WHEN M.PO_FULL_RCVD_DATE IS NOT NULL THEN 'RECEIVED'
               WHEN M.PO_DATE < DATEADD(HOUR, -26, CURRENT_TIMESTAMP()) AND M.P_PO_NO IS NULL THEN 'RECEIVED'
          END AS MILESTONE,
          CASE 
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) >= CT.RECEIVED_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'COLLECT' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) < CT.RECEIVED_R THEN 'RED'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) >= CT.RECEIVED_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'PREPAID' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) < CT.RECEIVED_R THEN 'RED'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) >= CT.RECEIVED_G THEN 'GREEN'
               WHEN M.PO_TYPE = 'INTERNATIONAL' AND MILESTONE = 'RECEIVED' AND DATEDIFF(DAY, M.PO_RCVD_DATE, M.DELIV_DATE) < CT.RECEIVED_R THEN 'RED' 
          END AS ADHERENCE_STATUS
          FROM M
          LEFT JOIN logistics.config_milestone_status CT ON M.PO_TYPE = CT.PO_TYPE
          ) WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
),

MILESTONES as (
     SELECT * FROM CREATED_POS
WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM MARKED_AS_READY_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM VENDOR_BOOKED_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM PLANNED_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM IN_TRANSIT_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM ON_YARD_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
UNION SELECT * FROM RECEIVED_POS
      WHERE MILESTONE IS NOT NULL AND MILESTONE != ''
)

,FIRST_AMBER AS (
	SELECT TC_PURCHASE_ORDERS_ID
		,MIN(MILESTONE_RANK) AS R
	FROM MILESTONES
	WHERE ADHERENCE_STATUS = 'AMBER'
	GROUP BY TC_PURCHASE_ORDERS_ID
	)
	,
FIRST_RED AS (
	SELECT TC_PURCHASE_ORDERS_ID
		,MIN(MILESTONE_RANK) AS R
	FROM MILESTONES
	WHERE ADHERENCE_STATUS = 'AMBER'
	GROUP BY TC_PURCHASE_ORDERS_ID
	),
MR AS (
    SELECT TC_PURCHASE_ORDERS_ID, MAX(MILESTONE_RANK) AS MAX_RANK FROM MILESTONES GROUP BY TC_PURCHASE_ORDERS_ID
),
SILVER_MILESTONES_QUERY as 
(
     SELECT 
          RM.TC_PURCHASE_ORDERS_ID AS ORDER_NO,
          RM.MILESTONE,
          ADHERENCE_STATUS,
          TC_SHIPMENT_ID,
          CASE WHEN RM.MILESTONE_RANK = MR.MAX_RANK THEN 'ACTIVE' ELSE 'INACTIVE' END AS ACTIVE_STATUS,
          CASE WHEN FIRST_AMBER.R = 1 THEN 'CREATED'
               WHEN FIRST_AMBER.R = 2 THEN 'VENDOR_BOOKED'
               WHEN FIRST_AMBER.R = 3 THEN 'MARKED_AS_READY'
               WHEN FIRST_AMBER.R = 4 THEN 'PLANNED'
               WHEN FIRST_AMBER.R = 5 THEN 'IN_TRANSIT'
               WHEN FIRST_AMBER.R = 6 THEN 'ON_YARD'
               WHEN FIRST_AMBER.R = 7 THEN 'RECEIVED' END AS FIRST_MILESTONE_TO_AMBER,
          CASE WHEN FIRST_RED.R = 1 THEN 'CREATED'
               WHEN FIRST_RED.R = 2 THEN 'VENDOR_BOOKED'
               WHEN FIRST_RED.R = 3 THEN 'MARKED_AS_READY'
               WHEN FIRST_RED.R = 4 THEN 'PLANNED'
               WHEN FIRST_RED.R = 5 THEN 'IN_TRANSIT'
               WHEN FIRST_RED.R = 6 THEN 'ON_YARD'
               WHEN FIRST_RED.R = 7 THEN 'RECEIVED' END AS FIRST_MILESTONE_TO_RED,
          EXPECTED_ETA_FOR_MILESTONE,
          ACTUAL_ETA_FOR_MILESTONE,
          NEXT_MILESTONE_EXPECTED_ETA,
          CURRENT_DELAY_IN_DAYS,
          REVISED_ETA_FOR_PO,
          RM.PO_TYPE,
          CURRENT_TIMESTAMP() AS CDC_TIMESTAMP,
          CASE WHEN PO_TYPE = 'COLLECT' OR PO_TYPE = 'PREPAID' THEN 'PO'
               WHEN PO_TYPE = 'INTERNATIONAL' THEN 'SHIPMENT' END AS RECORD_TYPE
     FROM MILESTONES RM
     LEFT JOIN FIRST_AMBER ON FIRST_AMBER.TC_PURCHASE_ORDERS_ID = RM.TC_PURCHASE_ORDERS_ID
     LEFT JOIN FIRST_RED ON FIRST_RED.TC_PURCHASE_ORDERS_ID = RM.TC_PURCHASE_ORDERS_ID
     LEFT JOIN MR ON RM.TC_PURCHASE_ORDERS_ID = MR.TC_PURCHASE_ORDERS_ID
)

SELECT *,
cast(null as boolean) as isCanceled,
cast(null as timestamp) as ETL_CREATED_DTTM,
cast(null as timestamp) as ETL_UPDATED_DTTM
FROM SILVER_MILESTONES_QUERY
"""

# COMMAND ----------

df_01_silver_inbound=spark.sql(qry)
if spark.catalog.tableExists("logistics.silver_tsc_pov_stg_milestone"):
  spark.sql("TRUNCATE TABLE logistics.silver_tsc_pov_stg_milestone")
df_01_silver_inbound.write.format("delta").mode("append").saveAsTable("logistics.silver_tsc_pov_stg_milestone")
