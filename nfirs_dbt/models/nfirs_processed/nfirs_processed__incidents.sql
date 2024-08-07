/***** NFIRS Processed Incidents Table Creation *****/
/***
The below model will join the following tables together to for further analysis:
- nfirs_transformed__fdheader
- nfirs_transformed__basicincident
- nfirs_transformed__incidentaddress
- nfirs_transformed__fireincident

The following logic will be applied:
- FD Header CTE to aggregate numeric columns
- Basic Incident CTE to select all columns from the respective table
- Fire Incident CTE to select all columns from the respective table
- The Basic Incident CTE serves at the base table and the FD Header and
  Fire Incident table are left joined to basic incident

All columns will be written out to clearly specify what columns are included
***/

{{ config(
    materialized='table',
    schema='processed'
) }}

-- Create FD Header CTEs
WITH transformed_fdheader_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__fdheader') }}
),

fdheader_processed AS (
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
    FROM transformed_fdheader_source
    GROUP BY ALL
),

-- Create Basic Incident CTEs
transformed_basicincident_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__basicincident') }}
),

basicincident_processed AS (
    SELECT * FROM transformed_basicincident_source
),

-- Create Fire Incident CTEs
transformed_fireincident_source AS (
    SELECT * FROM {{ ref('nfirs_transformed__fireincident') }}
),

fireincident_processed AS (
    SELECT * FROM transformed_fireincident_source
),

-- Create Incident Address CTEs
transformed_incidentaddress_source AS (
	SELECT * FROM {{ ref('nfirs_transformed__incidentaddress') }}
),

incidentaddress_processed AS (
	SELECT * FROM transformed_incidentaddress_source
),

-- Join the three processed CTEs together
incidents_processed_final AS (
    SELECT
        fd.state,
        fd.fdid,
        fd.fd_name,
        fd.fd_str_no,
        fd.fd_str_pre,
        fd.fd_street,
        fd.fd_str_typ,
        fd.fd_str_suf,
        fd.fd_city,
        fd.fd_zip,
        fd.fd_phone,
        fd.fd_fax,
        fd.fd_email,
        fd.fd_fip_cty,
		ad.loc_type,
		ad.num_mile,
		ad.street_pre,
		ad.streetname,
		ad.streettype,
		ad.streetsuf,
		ad.apt_no,
		ad.city,
		ad.state_id,
		ad.zip5,
		ad.zip4,
		ad.x_street,
        b.incident_key,
	    b.inc_date,
	    b.inc_no,
        b.exp_no,
        b.dept_sta,
        b.inc_type,
        b.add_wild,
        b.aid,
        b.alarm,
        b.arrival,
        b.inc_cont,
        b.lu_clear,
        b.shift,
        b.alarms,
        b.district,
		b.act_tak1,
		b.act_tak2,
		b.act_tak3,
		b.app_mod,
		b.sup_app,
		b.ems_app,
		b.oth_app,
		b.sup_per,
		b.ems_per,
		b.oth_per,
		b.resou_aid,
		b.prop_loss,
		b.cont_loss,
		b.prop_val,
		b.cont_val,
		b.ff_death,
		b.oth_death,
		b.ff_inj,
		b.oth_inj,
		b.det_alert,
		b.haz_rel,
		b.mixed_use,
		b.prop_use,
		b.census,
        f.num_unit,
		f.not_res,
		f.bldg_invol,
		f.acres_burn,
		f.less_1acre,
		f.on_site_m1,
		f.mat_stor1,
		f.on_site_m2,
		f.mat_stor2,
		f.on_site_m3,
		f.mat_stor3,
		f.area_orig,
		f.heat_sourc,
		f.first_ign,
		f.conf_orig,
		f.type_mat,
		f.cause_ign,
		f.fact_ign_1,
		f.fact_ign_2,
		f.hum_fac_1,
		f.hum_fac_2,
		f.hum_fac_3,
		f.hum_fac_4,
		f.hum_fac_5,
		f.hum_fac_6,
		f.hum_fac_7,
		f.hum_fac_8,
		f.age,
		f.sex,
		f.equip_inv,
		f.sup_fac_1,
		f.sup_fac_2,
		f.sup_fac_3,
		f.mob_invol,
		f.mob_type,
		f.mob_make,
		f.mob_model,
		f.mob_year,
		f.mob_lic_pl,
		f.mob_state,
		f.mob_vin_no,
		f.eq_brand,
		f.eq_model,
		f.eq_ser_no,
		f.eq_year,
		f.eq_power,
		f.eq_port,
		f.fire_sprd,
		f.struc_type,
		f.struc_stat,
		f.bldg_above,
		f.bldg_below,
		f.bldg_lgth,
		f.bldg_width,
		f.tot_sq_ft,
		f.fire_orig,
		f.st_dm_min_update,
		f.st_dm_sig,
		f.st_dam_hvy,
		f.st_dam_xtr,
		f.flame_sprd,
		f.item_sprd,
		f.mat_sprd,
		f.detector,
		f.det_type,
		f.det_power,
		f.det_operat,
		f.det_effect,
		f.det_fail,
		f.aes_pres,
		f.aes_type,
		f.aes_oper,
		f.no_spr_op,
		f.aes_fail
    FROM basicincident_processed b
	LEFT JOIN incidentaddress_processed ad
		ON b.incident_key = ad.incident_key
    LEFT JOIN fdheader_processed fd
        ON b.fdid = fd.fdid AND b.state = fd.state
    LEFT JOIN fireincident_processed f
        ON b.incident_key = f.incident_key AND b.inc_no = f.inc_no
)


SELECT * FROM incidents_processed_final