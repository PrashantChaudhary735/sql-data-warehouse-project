-- Building Gold Layer

SELECT cst_id, COUNT(*) FROM
(SELECT
ci.*,
ca.bdate,
ca.gen,
la.cntry 
FROM 
silver.crm_cust_info ci
left join 
silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid)t GROUP by cst_id
HAVING COUNT(*) > 1

use DataWarehouse;

Create schema gold;


-- Create gold layer view
CREATE VIEW gold.dim_customers as
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
CASE WHEN ci.cst_gndr != 'NA' THEN ci.cst_gndr
	 ELSE COALESCE(ca.gen, 'NA')
END As gender,
ca.bdate birthdate,
ci.cst_create_date create_date
FROM 
silver.crm_cust_info ci
left join 
silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid


SELECT distinct gender FROm gold.dim_customers; 

SELECT prd_key, COUNT(*) FROM 
(
SELECT
Pn.*,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver.crm_prd_info pn 
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
WHERE prd_end_dt is null
) t group by prd_key
having COUNT(*) > 1; 


SELECT COUNT(*) FROm silver.erp_px_cat_g1v2

