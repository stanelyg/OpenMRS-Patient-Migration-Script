-- client_flat for creating patients
 
SELECT m.id client_id,first_name as given_name,middle_name, last_name as family_name,'F' as gender,date_of_birth as birthdate,
0 as birthdate_estimated,0 as dead,1 as preferred,c.name county_district,sc.name state_province,w.name address4,village city_village,landmark address2,
informal_settlement address5,phone_number as telephone_number, SUBSTRING_INDEX(guardian_name, ' ', 1) Guardian_First_Name,
CASE
    WHEN LENGTH(guardian_name) - LENGTH(REPLACE(guardian_name, ' ', '')) >= 2 THEN SUBSTRING_INDEX(guardian_name, ' ', -1)
    WHEN LENGTH(guardian_name) - LENGTH(REPLACE(guardian_name, ' ', '')) = 1 THEN SUBSTRING_INDEX(guardian_name, ' ', -1)
    ELSE SUBSTRING_INDEX(guardian_name, ' ', 1)
  END AS Guardian_last_name,
  CASE  
      WHEN vrd.verification_document_id = 1 THEN vrd.verification_doc_no
  END AS Birth_Certificate_Number,
  CASE  
      WHEN vrd.verification_document_id = 2 THEN vrd.verification_doc_no
  END AS National_ID
FROM dreams_production.DreamsApp_client m
left join DreamsApp_county c on c.id=m.county_of_residence_id
left join dreamsapp_subcounty sc on sc.id=m.sub_county_id
left join dreamsapp_ward w on w.id=m.ward_id
left join (
  select m.id ,verification_doc_no,verification_document_id,vd.name from dreamsapp_client m
  left join dreamsapp_verificationdocument vd on vd.id=m.verification_document_id
  where verification_doc_no <> ''  and verification_document_id <> '' and name <> '' and vd.id in (1,2)
)vrd on vrd.id=m.id
WHERE m.voided=0 and m.exited=1 limit 10;


--  Demographics Enrolment flat TABLE for migrating the enrolments
SELECT implementing_partner_id,date_of_enrollment,verification_document_id,verification_document_other,verification_doc_no,marital_status_id
,phone_number,dss_id_number,county_of_residence_id,sub_county_id,ward_id,informal_settlement,village,landmark,dreams_id,guardian_name,relationship_with_guardian,
guardian_phone_number,guardian_national_id,external_organisation_id,cpmis_id,nemis_no,nupi_no
 FROM dreams_production.dreamsapp_client
 left join 
 where voided=0 ;
 
  -- household
SELECT client_id,head_of_household_id,head_of_household_other,age_of_household_head,is_father_alive,is_mother_alive,is_parent_chronically_ill
,main_floor_material_id,main_roof_material_id,main_roof_material_other,main_wall_material_id,main_wall_material_other
,source_of_drinking_water_id,source_of_drinking_water_other,no_of_days_missed_food_in_4wks_id,has_disability_id,disabilitytype_id,disability_type_other,no_of_people_in_household,no_of_females,no_of_males,
no_of_children,no_of_adults,ever_enrolled_in_ct_program_id,currently_in_ct_program_id,current_ct_program
 FROM dreams_production.dreamsapp_clientindividualandhouseholddata hd
 Inner join dreamsapp_client c on hd.client_id=c.id
 Left join dreamsapp_clientindividualandhouseholddata_disability_type dt on hd.id=dt.clientindividualandhouseholddata_id where voided=0;
 
 
 -- Education and Employment
 
SELECT client_id,currently_in_school_id,current_school_name,current_school_type_id,current_school_level_id,current_school_level_other,
current_class,educationsupporter_id,current_education_supporter_other,current_income_source_id,current_income_source_other,has_savings_id,banking_place_id,banking_place_other,
reason_not_in_school_id,reason_not_in_school_other,last_time_in_school_id,dropout_school_level_id,dropout_class,life_wish_id,life_wish_other
FROM dreams_production.dreamsapp_clienteducationandemploymentdata ce
left join dreamsapp_clienteducationandemploymentdata_current_educationebf4 esp on ce.id=esp.clienteducationandemploymentdata_id where c.voided=0;
 
 
 -- hiv testing 
SELECT client_id,ever_tested_for_hiv_id,period_last_tested_id,last_test_result_id,ccc_no,enrolled_in_hiv_care_id,care_facility_enrolled,reason_not_in_hiv_care_id,reason_not_in_hiv_care_other,
reasonnottestedforhiv_id,reason_never_tested_for_hiv_other,knowledge_of_hiv_test_centres_id
from dreams_production.dreamsapp_clienthivtestingdata ht
left join dreamsapp_clienthivtestingdata_reason_never_tested_for_hiv rnt on ht.id=rnt.clienthivtestingdata_id where voided=0;

--sexual activity
SELECT client_id,ever_had_sex_id,age_at_first_sexual_encounter,has_sexual_partner_id,sex_partners_in_last_12months,age_of_last_partner_id,
age_of_second_last_partner_id,age_of_third_last_partner_id,last_partner_circumcised_id,second_last_partner_circumcised_id,third_last_partner_circumcised_id,
know_last_partner_hiv_status_id,know_second_last_partner_hiv_status_id,know_third_last_partner_hiv_status_id
used_condom_with_last_partner_id,used_condom_with_second_last_partner_id,used_condom_with_third_last_partner_id,received_money_gift_for_sex_id
FROM dreams_production.dreamsapp_clientsexualactivitydata where voided=0;

--reproductive health
SELECT client_id,has_biological_children_id,no_of_biological_children,currently_pregnant_id,current_anc_enrollment_id,anc_facility_name,fp_methods_awareness_id
,familyplanningmethod_id,known_fp_method_other,currently_use_modern_fp_id,current_fp_method_id,current_fp_method_other,reason_not_using_fp_id,reason_not_using_fp_other
 FROM dreams_production.dreamsapp_clientreproductivehealthdata rp
 Left join dreamsapp_clientreproductivehealthdata_known_fp_method rpm on rp.id=rpm.clientreproductivehealthdata_id where voided=0;
 
 --violence against women
SELECT client_id,humiliated_ever_id,humiliated_last_3months_id,threats_to_hurt_ever_id,threats_to_hurt_last_3months_id,insulted_ever_id,insulted_last_3months_id 
,economic_threat_ever_id,economic_threat_last_3months_id,physical_violence_ever_id,physical_violence_last_3months_id,physically_forced_sex_ever_id,physically_forced_sex_last_3months_id,
physically_forced_other_sex_acts_ever_id,physically_forced_other_sex_acts_last_3months_id,threatened_for_sexual_acts_ever_id,threatened_for_sexual_acts_last_3months_id,hp.gbvhelpprovider_id,
gbv_help_provider_other,php.gbvhelpprovider_id as preferred_gbv_help_provider,preferred_gbv_help_provider_other
FROM dreams_production.dreamsapp_clientgenderbasedviolencedata gb
Left join dreamsapp_clientgenderbasedviolencedata_gbv_help_provider hp on gb.id=hp.clientgenderbasedviolencedata_id
left join dreamsapp_clientgenderbasedviolencedata_preferred_gbv_help_p1bce php on gb.id=php.clientgenderbasedviolencedata_id where voided=0;

--drug use
SELECT client_id,used_alcohol_last_12months_id,frequency_of_alcohol_last_12months_id,drug_abuse_last_12months_id,du.drug_id,drug_abuse_last_12months_other,
drug_used_last_12months_other,produced_alcohol_last_12months_id
FROM dreams_production.dreamsapp_clientdrugusedata cd
left join dreamsapp_clientdrugusedata_drug_used_last_12months du on cd.id=du.clientdrugusedata_id
where voided=0;

 
SELECT client_id,dreamsprogramme_id,dreams_program_other FROM dreams_production.dreamsapp_clientparticipationindreams m
left join dreamsapp_clientparticipationindreams_dreams_program pms on m.id=pms.clientparticipationindreams_id
where voided=0;




-- query to get the concepts_uuids and labels
SELECT distinct m.uuid as concept,nm.name as label FROM openmrs.concept m
inner join concept_name nm on nm.concept_id=m.concept_id
where m.concept_id in (SELECT concept_id FROM openmrs.dreamsapp_subcounty) order by nm.name;
 
 CREATE TABLE `openmrs`.`dreams_householdname_mapping` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `original_id` INT NULL,
  `name` VARCHAR(45) NULL,
  `concept_id` VARCHAR(45) NULL,
  PRIMARY KEY (`id`));
	INSERT INTO `openmrs`.`dreams_householdname_mapping` (`original_id`, `name`, `concept_id`) VALUES
	(1, 'Self', NULL),
	(2, 'Father', NULL),
	(3, 'Mother', NULL),
	(4, 'Sibling', NULL),
	(5, 'Uncle/Aunt', NULL),
	(6, 'Grandparents', NULL),
	(7, 'Husband/Partner', NULL),
	(96, 'Other/Specify', NULL);
	
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '978' WHERE (`id` = '1');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '971' WHERE (`id` = '2');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '970' WHERE (`id` = '3');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '972' WHERE (`id` = '4');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '974' WHERE (`id` = '5');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '973' WHERE (`id` = '6');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '1001684' WHERE (`id` = '7');
	UPDATE `openmrs`.`dreams_householdname_mapping` SET `concept_id` = '1001280' WHERE (`id` = '8');

-- create  visits flat TABLE for enrolments

CREATE TABLE enrollement_visits_flat
SELECT client_id,patient_id,  date_of_enrollment as date_started, date_of_enrollment as date_stopped
FROM dreams_production.DreamsApp_client c
Inner join dreams_client_patient_mapping cp on c.id=cp.client_id
where c.voided=0 ;

--visits mapping 
CREATE TABLE `dreams_production`.`patient_visits_mapping` (
  `id` INT NOT NULL,
  `patient_id` INT NULL,
  `visit_id` INT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `patient_id_UNIQUE` (`patient_id` ASC) VISIBLE,
  UNIQUE INDEX `visit_id_UNIQUE` (`visit_id` ASC) VISIBLE);




---create encounters
CREATE TABLE `dreams_production`.`patient_encounter_mapping` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `patient_id` INT NULL,
  `encounter_id` INT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `patient_id_UNIQUE` (`patient_id` ASC) VISIBLE,
  UNIQUE INDEX `encounter_id_UNIQUE` (`encounter_id` ASC) VISIBLE);


  -- person 
gender
birthdate
birthdate_estimated =0
dead=0
creator =1
date_created = NOW()
UUID()
--person_name
preferred=1
given_name=first_name
middle_name =middle_name
family_name=last_name
creator =1
date_created = NOW()
UUID()

--person_address
county=county_district
subcounty=state_province
ward=address4
village=city_village
landmark_near_r=address2
inform_settlement=address5
uuid, 
creator, 
date_created
--person_attribute_type
Guardian first name
Guardian last name
telephone number



--insert into patient
INSERT INTO patient (patient_id, creator, date_created, uuid)
VALUES (LAST_INSERT_ID(), 1, NOW(), UUID());


