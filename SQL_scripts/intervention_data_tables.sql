-- behavioural_interven
		DROP TABLE if exists tbl_behavioural_interven;
		CREATE TABLE tbl_behavioural_interven
		SELECT client_id,c.implementing_partner_id,intervention_type_id,intervention_date,comment,
		CASE 
		  WHEN intervention_type_id=7 THEN name_specified
		  ELSE
		   NUll
		END as other_specify
		FROM dreams_production.DreamsApp_intervention i 
		INNER JOIN DreamsApp_interventiontype it ON it.id=i.intervention_type_id
		INNER JOIN DreamsApp_client c on c.id=i.client_id
		where it.intervention_category_id=1 and i.voided=0 and c.implementing_partner_id NOT IN (37,35,39);

		-- Bio medical
		DROP TABLE if exists tbl_biomedical_interven;
			CREATE TABLE tbl_biomedical_interven
			SELECT client_id,c.implementing_partner_id,intervention_date,intervention_type_id,hts_result_id,pregnancy_test_result_id,client_ccc_number,date_linked_to_ccc,comment,
			CASE 
			  WHEN intervention_type_id=67 THEN name_specified
			  ELSE
			   NUll
		END as other_specify
		FROM dreams_production.DreamsApp_intervention i 
		INNER JOIN DreamsApp_interventiontype it ON it.id=i.intervention_type_id
		INNER JOIN DreamsApp_client c on c.id=i.client_id
		where it.intervention_category_id=2 and i.voided=0 and c.implementing_partner_id NOT IN (37,35,39) ;


		-- PVC medical
		DROP TABLE if exists tbl_pvc_interventions;
		CREATE TABLE tbl_pvc_interventions
		SELECT client_id,c.implementing_partner_id,intervention_type_id,intervention_date,comment,
		CASE 
		  WHEN intervention_type_id=59 THEN name_specified
		  ELSE
		   NUll
		END as sexual_violence_others,
		CASE 
			  WHEN intervention_type_id=66 THEN name_specified
			  ELSE
			   NUll
		END as physical_violence_others,
		CASE 
			  WHEN intervention_type_id=103 THEN name_specified
			  ELSE
			   NUll
		END as emotional_violence_others
		FROM dreams_production.DreamsApp_intervention i 
		INNER JOIN DreamsApp_interventiontype it ON it.id=i.intervention_type_id
		INNER JOIN DreamsApp_client c on c.id=i.client_id
		where it.intervention_category_id=3 and c.voided=0 and i.voided=0 and c.implementing_partner_id NOT IN (37,35,39);

		-- social protection

		DROP TABLE if exists tbl_social_protection_interventions;
		CREATE TABLE tbl_social_protection_interventions
		SELECT client_id,c.implementing_partner_id,intervention_type_id,intervention_date,comment,
		CASE 
		  WHEN intervention_type_id=68 THEN name_specified
		  ELSE
		   NUll
		END as other_specify
		FROM dreams_production.DreamsApp_intervention i 
		INNER JOIN DreamsApp_interventiontype it ON it.id=i.intervention_type_id
		INNER JOIN DreamsApp_client c on c.id=i.client_id
		where it.intervention_category_id=4 and c.voided=0 and i.voided=0 and c.implementing_partner_id NOT IN (37,35,39);


		-- other service

		DROP TABLE if exists tbl_other_interventions;
		CREATE TABLE tbl_other_interventions
		SELECT client_id,c.implementing_partner_id,intervention_type_id,intervention_date,comment,
		CASE 
		  WHEN intervention_type_id=81 THEN name_specified
		  ELSE
		   NUll
		END as referrals
		FROM dreams_production.DreamsApp_intervention i 
		INNER JOIN DreamsApp_interventiontype it ON it.id=i.intervention_type_id
		INNER JOIN DreamsApp_client c on c.id=i.client_id
		where it.intervention_category_id=5 and c.voided=0 and i.voided=0 and c.implementing_partner_id NOT IN (37,35,39);    
        -- tools data 
        DROP TABLE IF EXISTS tbl_tools_data;
        CREATE TABLE tbl_tools_data
        SELECT m.id,post_date,formcatalogue_id,client_id,fd.text_response,fd.option_item_id,fd.question_id 
		FROM dreams_production.DreamsApp_formmasterdata m
		INNER JOIN DreamsApp_formdatadetail fd on fd.formmasterdata_id=m.id
		INNER JOIN DreamsApp_client c ON c.id=m.client_id
		where formcatalogue_id IN (1,4,5,6,7,8,10,11,14,16) and m.voided =0 and c.implementing_partner_id NOT IN (37,35,39);
