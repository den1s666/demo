-- 3. vytvorenie tabulky so spravnym klucom
CREATE TABLE o4_my_organization_table (bscs_code varchar, type varchar)
WITH (kafka_topic = 'my_o4_organization_bscscode', value_format = 'delimited', key = 'bscs_code', PARTITIONS=1);


-- 4. vytvorenie streamu pre zdrojovy topic - so zachovanim struktury
CREATE STREAM o2_my_interaction_activity_stream_to_enrich (
    activityBusinessId VARCHAR, activityId VARCHAR, applicationDN VARCHAR,
        assistantLogin VARCHAR, callId VARCHAR, customerVisibility BOOLEAN,
        description VARCHAR, activityCode VARCHAR, interactionId VARCHAR,
        labels ARRAY<STRING>, labels_string STRING, loginMSISDN VARCHAR,
        organizationId VARCHAR, referenceBACuRefNo VARCHAR, referenceCustomerCuRefNo VARCHAR,
        referenceEntityId VARCHAR, referenceEntityStatus VARCHAR, referenceEntityType VARCHAR,
        referenceSubscriberId VARCHAR, referenceSubscriberMSISDN VARCHAR, referenceSubscriberType VARCHAR,
        relatedInteractionId VARCHAR, serviceProvider VARCHAR, timestamp VARCHAR,
        userAccountId VARCHAR, userLogin VARCHAR, visibility VARCHAR,
        attributes MAP<STRING, STRING>)
WITH (KAFKA_TOPIC='my_o2_prod_interactionactivity', VALUE_FORMAT='json', PARTITIONS=1);

-- 5. pridame ne-null-ovy atribut pre organizationId
CREATE STREAM o2_my_interaction_activity_stream_normalized WITH
(VALUE_FORMAT='JSON', KAFKA_TOPIC='o2_my_interaction_activity_stream_normalized')
AS SELECT *, IFNULL(organizationId, '') as organizationId_nonNull FROM o2_my_interaction_activity_stream_to_enrich;

-- 6. realizujeme join streamu s ciselnikom ORGANiZATION a aplikovanie globalnych uprav nad datami potrebnymi k ukladaniu dat do exponea
--STREAM 1
CREATE STREAM o2_my_interaction_activity_stream_with_organization_id WITH (PARTITIONS=1)
    AS SELECT
            s.activityBusinessId AS activityBusinessId, s.activityId AS activityId, s.applicationDN AS applicationDN,
            s.assistantLogin AS assistantLogin,
            CASE
               WHEN IFNULL(s.assistantLogin, 'N') = 'N' THEN 'N'
               ELSE 'Y'
            END AS asistant_flag,
            s.callId AS callId, s.customerVisibility AS customerVisibility,
            s.description AS description,
            SPLIT(LCASE(s.description), '|')[0] AS interaction_type,
            s.activityCode AS activityCode, s.interactionId AS interactionId,
            s.labels AS labels,
            substring(cast(labels as string), 2, len(cast(labels as string))-2) AS labels2string,
            s.labels_string AS labels_string, s.loginMSISDN AS loginMSISDN,
            s.organizationId AS organizationId,
            CASE
               WHEN IFNULL(s.organizationId, 'N') = 'N' THEN 'N'
               ELSE 'Y'
            END AS shop_flag,
            s.referenceBACuRefNo AS referenceBACuRefNo, s.referenceCustomerCuRefNo AS referenceCustomerCuRefNo,
            s.referenceEntityId AS referenceEntityId, s.referenceEntityStatus AS referenceEntityStatus, s.referenceEntityType AS referenceEntityType,
            s.referenceSubscriberId AS referenceSubscriberId, CONCAT('0',s.referenceSubscriberMSISDN) AS referenceSubscriberMSISDN, s.referenceSubscriberType AS referenceSubscriberType,
            s.relatedInteractionId AS relatedInteractionId, s.serviceProvider AS serviceProvider,
            substring(cast(STRINGTOTIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ssZ') AS string),1,10) + '.' + substring(cast(STRINGTOTIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ssZ') AS string),11,3) AS timestamp, timestamp AS date,
            s.userAccountId AS userAccountId, s.userLogin AS userLogin, s.visibility AS visibility,
            s.attributes AS attributes, IFNULL(s.attributes['lineItemPartNum'],null) as lineItemPartNum, IFNULL(t.type, 'NULL') as organization_type
        FROM o2_my_interaction_activity_stream_normalized s
            LEFT JOIN o4_my_organization_table t ON s.organizationId_nonNull = t.bscs_code
                WHERE referenceSubscriberMSISDN IS NOT NULL AND (serviceprovider = 'TM' OR serviceprovider = 'O2');

-- 6. b ulozenie dat do topicu
--@DeleteTopic
CREATE STREAM o2_my_interaction_activity_stream_materialized
WITH (kafka_topic = 'O2.MY.CRM.INTERACTIONACTIVITY.NORM.MATERIALIZED', PARTITIONS=2, REPLICAS=1)
    AS SELECT * from o2_my_interaction_activity_stream_with_organization_id;
