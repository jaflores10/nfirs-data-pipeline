/**** Logic for NFIRS Fire Incident Details with Code Lookup table

The below logic will combine several tables FROM the 'nfirs_transformed' schema to create a processed table which can be used for analysis.
Specifically, the following tables will be included:
- FD Header
- Basic Incident
- Fire Incident
***/

WITH fdheader_cte AS (
	SELECT
		state,
		fdid,
		fd_name,
		fd_str_no,
		fd_str_pre,
		fd_street,
		fd_str_typ,
		fd_str_suf,
		fd_city,
		fd_zip,
		fd_phone,
		fd_fax,
		fd_email,
		fd_fip_cty,
		SUM(no_station) AS no_station,
		SUM(no_pd_ff) AS no_pd_ff,
		SUM(no_vol_ff) AS no_vol_ff,
		SUM(no_vol_pdc) AS no_vol_pdc
	FROM nfirs_transformed.nfirs_transformed__fdheader
	GROUP BY ALL
),

fdheader_basicincident_cte AS (
	SELECT
		fd.*,
		bi.incident_key,
		bi.inc_date,
		bi.inc_no,
		bi.exp_no,
		bi.dept_sta,
		bi.inc_type,
		bi.add_wild,
		bi.aid,
		bi.alarm,
		bi.arrival,
		bi.inc_cont,
		bi.lu_clear,
		bi.shift,
		bi.alarms,
		bi.district,
		bi.act_tak1,
		bi.act_tak2,
		bi.act_tak3,
		bi.app_mod,
		bi.sup_app,
		bi.ems_app,
		bi.oth_app,
		bi.sup_per,
		bi.ems_per,
		bi.oth_per,
		bi.resou_aid,
		bi.prop_loss,
		bi.cont_loss,
		bi.prop_val,
		bi.cont_val,
		bi.ff_death,
		bi.oth_death,
		bi.ff_inj,
		bi.oth_inj,
		bi.det_alert,
		bi.haz_rel,
		bi.mixed_use,
		bi.prop_use,
		bi.census
	FROM nfirs_transformed.nfirs_transformed__basicincident bi
	LEFT JOIN fdheader_cte fd
		ON bi.fdid = fd.fdid AND bi.state = fd.state
),


incidents_cte AS (
	SELECT
		fb.*,
		fr.num_unit,
		fr.not_res,
		fr.bldg_invol,
		fr.acres_burn,
		fr.less_1acre,
		fr.on_site_m1,
		fr.mat_stor1,
		fr.on_site_m2,
		fr.mat_stor2,
		fr.on_site_m3,
		fr.mat_stor3,
		fr.area_orig,
		fr.heat_sourc,
		fr.first_ign,
		fr.conf_orig,
		fr.type_mat,
		fr.cause_ign,
		fr.fact_ign_1,
		fr.fact_ign_2,
		fr.hum_fac_1,
		fr.hum_fac_2,
		fr.hum_fac_3,
		fr.hum_fac_4,
		fr.hum_fac_5,
		fr.hum_fac_6,
		fr.hum_fac_7,
		fr.hum_fac_8,
		fr.age,
		fr.sex,
		fr.equip_inv,
		fr.sup_fac_1,
		fr.sup_fac_2,
		fr.sup_fac_3,
		fr.mob_invol,
		fr.mob_type,
		fr.mob_make,
		fr.mob_model,
		fr.mob_year,
		fr.mob_lic_pl,
		fr.mob_state,
		fr.mob_vin_no,
		fr.eq_brand,
		fr.eq_model,
		fr.eq_ser_no,
		fr.eq_year,
		fr.eq_power,
		fr.eq_port,
		fr.fire_sprd,
		fr.struc_type,
		fr.struc_stat,
		fr.bldg_above,
		fr.bldg_below,
		fr.bldg_lgth,
		fr.bldg_width,
		fr.tot_sq_ft,
		fr.fire_orig,
		fr.st_dm_min_update,
		fr.st_dm_sig,
		fr.st_dam_hvy,
		fr.st_dam_xtr,
		fr.flame_sprd,
		fr.item_sprd,
		fr.mat_sprd,
		fr.detector,
		fr.det_type,
		fr.det_power,
		fr.det_operat,
		fr.det_effect,
		fr.det_fail,
		fr.aes_pres,
		fr.aes_type,
		fr.aes_oper,
		fr.no_spr_op,
		fr.aes_fail
	FROM fdheader_basicincident_cte fb
	LEFT JOIN nfirs_transformed.nfirs_transformed__fireincident fr
		ON fb.incident_key = fr.incident_key AND fb.inc_no = fr.inc_no
),

inc_type_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'INC_TYPE'
),

act_tak1_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'ACT_TAK1'
),

act_tak2_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'ACT_TAK2'
),

act_tak3_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'ACT_TAK3'
),

sup_fac_1_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'SUP_FAC_1'
),

sup_fac_2_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'SUP_FAC_2'
),

sup_fac_3_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'SUP_FAC_3'
),

detector_type_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DETECTOR'
),

det_type_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DET_TYPE'
),

det_power_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DET_POWER'
),

det_operat_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DET_OPERAT'
),

det_effect_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DET_EFFECT'
),

det_fail_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'DET_FAIL'
),

aes_pres_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'AES_PRES'
),

aes_type_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'AES_TYPE'
),

aes_oper_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'AES_OPER'
),

aes_fail_code_lookup_cte AS (
	SELECT
		*
	FROM nfirs_transformed.nfirs_transformed__codelookup
	WHERE
		fieldid = 'AES_FAIL'
),



final_cte AS (
	SELECT
		inc.*,
		inc_type.code_descr AS inc_type_code_descr,
		act_tak1.code_descr AS act_tak1_code_descr,
		act_tak2.code_descr AS act_tak2_code_descr,
		act_tak3.code_descr AS act_tak3_code_descr,
		sup_fac_1.code_descr AS sup_fac_1_code_descr,
		sup_fac_2.code_descr AS sup_fac_2_code_descr,
		sup_fac_3.code_descr AS sup_fac_4_code_descr,
		det.code_descr AS detector_code_descr,
		det_type.code_descr AS det_type_code_descr,
		det_power.code_descr AS det_power_code_descr,
		det_operat.code_descr AS det_operat_code_descr,
		det_fail.code_descr AS det_fail_code_descr,
		aes_pres.code_descr AS aes_pres_code_descr,
		aes_type.code_descr AS aes_type_code_descr,
		aes_oper.code_descr AS aes_oper_code_descr,
		aes_fail.code_descr AS aes_fail_code_descr,
	FROM incidents_cte inc
	
	LEFT JOIN inc_type_code_lookup_cte inc_type
		ON inc.inc_type = inc_type.code_value
	
	LEFT JOIN act_tak1_code_lookup_cte act_tak1
		ON inc.act_tak1 = act_tak1.code_value
		
	LEFT JOIN act_tak2_code_lookup_cte act_tak2
		ON inc.act_tak2 = act_tak2.code_value
	
	LEFT JOIN act_tak3_code_lookup_cte act_tak3
		ON inc.act_tak3 = act_tak3.code_value
	
	LEFT JOIN sup_fac_1_code_lookup_cte sup_fac_1
		ON inc.sup_fac_1 = sup_fac_1.code_value
	
	LEFT JOIN sup_fac_2_code_lookup_cte sup_fac_2
		ON inc.sup_fac_2 = sup_fac_2.code_value
	
	LEFT JOIN sup_fac_3_code_lookup_cte sup_fac_3
		ON inc.sup_fac_3 = sup_fac_3.code_value
	
	LEFT JOIN detector_type_code_lookup_cte det
		ON inc.detector = det.code_value
	
	LEFT JOIN det_type_code_lookup_cte det_type
		ON inc.det_type = det_type.code_value
	
	LEFT JOIN det_power_code_lookup_cte det_power
		ON inc.det_power = det_power.code_value
	
	LEFT JOIN det_operat_code_lookup_cte det_operat
		ON inc.det_operat = det_operat.code_value
		
	LEFT JOIN det_fail_code_lookup_cte det_fail
		ON inc.det_fail = det_fail.code_value
		
	LEFT JOIN aes_pres_code_lookup_cte aes_pres
		ON inc.aes_pres = aes_pres.code_value
		
	LEFT JOIN aes_type_code_lookup_cte aes_type
		ON inc.aes_pres = aes_type.code_value
		
	LEFT JOIN aes_oper_code_lookup_cte aes_oper
		ON inc.aes_oper = aes_oper.code_value
		
	LEFT JOIN aes_fail_code_lookup_cte aes_fail
		ON inc.aes_fail = aes_fail.code_value

)
		
SELECT 
	'final_cte' AS view,
	COUNT(*) 
FROM final_cte

UNION ALL

SELECT
	'incidents',
	COUNT(*)
FROM incidents_cte
/*SELECT 
	incident_key,
	inc_type AS inc_type_og,
	inc_type_code_value,
	code_descr
FROM final_cte
*/