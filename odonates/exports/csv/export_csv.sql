DROP VIEW IF EXISTS gn_monitoring.v_export_odonates_standard;
CREATE OR REPLACE VIEW gn_monitoring.v_export_odonates_standard AS WITH module AS (
        SELECT *
        FROM gn_commons.t_modules tm
        WHERE module_code = 'odonates'
    ),
    sites AS (
        SELECT tbs.id_base_site,
            sc.id_module as id_module,
            tbs.base_site_name as nompzh,
            tbs.base_site_code,
            tbs.base_site_description,
            tbs.id_inventor,
            tbs.id_digitiser,
            COALESCE (tbs.meta_update_date, first_use_date) AS date_site,
            tbs.altitude_min,
            tbs.altitude_max,
            tbs.geom_local,
            st_x(ST_Centroid(geom)) AS wgs84_x,
            st_y(ST_Centroid(geom)) AS wgs84_y,
            st_x(ST_Centroid(geom_local)) AS l93_x,
            st_y(ST_Centroid(geom_local)) AS l93_y,
            (sc.data::json#>>'{pzhCode}')::text AS pzhCode,
            (roles.nom_role || ' ' || roles.prenom_role) AS numer_site,
            org.nom_organisme AS organisme,
            ARRAY_AGG(dep.area_name) AS departement,
            ARRAY_AGG(dep.area_code) AS code_dep,
            ARRAY_AGG(com.area_name) AS commune,
            (sc.data::json#>>'{qObserver}')::text AS qObserver,
            ARRAY(
                select tr.nom_role || ' ' || tr.prenom_role
                from unnest(
                        ARRAY(
                            SELECT jsonb_array_elements_text(sc.data->'observateurCompl')::int
                        )
                    ) entity_id
                    join utilisateurs.t_roles tr ON tr.id_role = entity_id
            ) as observateurCompl,
            (sc.data::json#>>'{observateurCompl}')::text AS observateurCompl2,
            (sc.data::json#>>'{gestionnaire}')::text AS gestionnairepzh,
            (sc.data::json#>>'{owner}')::text AS proprietairepzh,
            (sc.data::json#>>'{proprietepzh}')::text AS proprietepzh,
            -- (sc.data::json#>>'{typeZH}')::text AS typezh,
            tn2.label_fr typezh,
            tn3.label_fr methodepzh
        FROM gn_monitoring.t_base_sites as tbs
            JOIN gn_monitoring.t_site_complements sc on sc.id_base_site = tbs.id_base_site -- LEFT JOIN ref_nomenclatures.t_nomenclatures tn1 ON tn1.id_nomenclature::text = (sc.data->>'typeZH')::text
            JOIN ref_nomenclatures.t_nomenclatures tn2 ON tn2.id_nomenclature::int = tbs.id_nomenclature_type_site
            JOIN ref_nomenclatures.t_nomenclatures tn3 ON tn3.id_nomenclature::text = (sc.data->>'id_nomenclature_prospection')::text
            JOIN utilisateurs.t_roles roles ON roles.id_role = tbs.id_digitiser
            JOIN utilisateurs.bib_organismes org ON org.id_organisme = roles.id_organisme
            JOIN (
                select la.area_name,
                    csa.id_base_site
                FROM ref_geo.l_areas la
                    JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
                    JOIN gn_monitoring.cor_site_area csa ON csa.id_area = la.id_area
                WHERE bat.type_code = 'COM'
            ) com ON tbs.id_base_site = com.id_base_site
            JOIN (
                select la.area_name,
                    la.area_code,
                    csa.id_base_site
                FROM ref_geo.l_areas la
                    JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
                    JOIN gn_monitoring.cor_site_area csa ON csa.id_area = la.id_area
                WHERE bat.type_code = 'DEP'
            ) dep ON tbs.id_base_site = dep.id_base_site
        GROUP BY tbs.id_base_site,
            numer_site,
            id_module,
            organisme,
            sc.data,
            typezh,
            methodepzh
    ),
    visites AS (
        SELECT tbv.id_base_site,
            tbv.id_module,
            tbv.id_base_visit,
            STRING_AGG(
                tr_digi.nom_role || ' ' || tr_digi.prenom_role,
                ', '
                ORDER BY tr_digi.nom_role,
                    tr_digi.prenom_role
            ) AS numer_visit,
            string_agg(
                DISTINCT concat (UPPER(tr.nom_role), ' ', tr.prenom_role),
                ', '
                ORDER BY concat (UPPER(tr.nom_role), ' ', tr.prenom_role)
            ) AS observers_visit,
            (tvc.data::json#>>'{qObserver}')::text AS qObserver_visit,
            ARRAY(
                select tr.nom_role || ' ' || tr.prenom_role
                from unnest(
                        ARRAY(
                            SELECT jsonb_array_elements_text(tvc.data->'observateurCompl')::int
                        )
                    ) entity_id
                    join utilisateurs.t_roles tr ON tr.id_role = entity_id
            ) as observateurCompl_visit,
            org.nom_organisme AS organisme_numer_visit,
            tbv.visit_date_min as date_visit,
            (tvc.data::json#>>'{heureDebut}')::text AS heureDebut,
            (tvc.data::json#>>'{heureFin}')::text AS heureFin,
            (tvc.data::json#>>'{passage}')::text AS passage,
            (tvc.data::json#>>'{periode}')::text AS periode,
            (tvc.data::json#>>'{tempAir}')::text AS tempAir,
            (tvc.data::json#>>'{humidite}')::text AS humidite,
            (tvc.data::json#>>'{pluviosite}')::text AS pluviosite,
            (tvc.data::json#>>'{couvertureNuageuse}')::text AS couvertureNuageuse,
            (tvc.data::json#>>'{vent}')::text AS vent,
            (tvc.data::json#>>'{pertubations}')::text AS pertubations,
            (tvc.data::json#>>'{comments}')::text AS commentaireVisite
        FROM gn_monitoring.t_base_visits tbv
            JOIN gn_monitoring.t_visit_complements tvc ON tvc.id_base_visit = tbv.id_base_visit
            JOIN gn_monitoring.cor_visit_observer cvo ON cvo.id_base_visit = tbv.id_base_visit
            JOIN utilisateurs.t_roles tr ON tr.id_role = cvo.id_role
            JOIN utilisateurs.t_roles tr_digi ON tr_digi.id_role = tbv.id_digitiser
            JOIN utilisateurs.bib_organismes org ON org.id_organisme = tr_digi.id_organisme -- WHERE
            --     DATE_PART('YEAR', tbv.visit_date_min) = DATE_PART('YEAR', current_timestamp) -1
        GROUP BY tbv.id_base_site,
            tbv.id_base_visit,
            tvc.data,
            org.nom_organisme
    )
select s.numer_site numer_site,
    s.organisme,
    s.departement,
    s.code_dep,
    s.commune,
    s.l93_x as longitude,
    s.l93_y as latitude,
    s.date_site as date_site,
    s.qObserver,
    s.proprietairepzh,
    s.proprietepzh,
    s.observateurCompl,
    s.gestionnairepzh,
    s.pzhCode,
    s.typezh,
    s.methodepzh,
    v.observateurCompl_visit,
    v.organisme_numer_visit,
    v.date_visit,
    v.heureDebut,
    v.heureFin,
    v.passage,
    v.periode,
    v.tempAir,
    v.humidite,
    v.pluviosite,
    v.couvertureNuageuse,
    v.vent,
    v.pertubations,
    v.commentaireVisite,
    t.lb_nom as nomScientifiqueRef,
    t.cd_nom as CD_nom,
    t.regne as Regne,
    t.classe as Classe,
    t.ordre as Ordre,
    t.famille as Famille,
    t.sous_famille as Genre,
    t.nom_complet as nomCite,
    t.nom_vern as nomVernaculaire,
    tn.label_fr as ocEtatBio,
    tn1.label_fr as ocStade,
    tn2.label_fr as ocSexe,
    (toc.data::json#>>'{count_exact}')::text as countExact,
    (toc.data::json#>>'{count_average}')::text as count_average,
    (toc.data::json#>>'{count_min}')::text as count_min,
    (toc.data::json#>>'{count_max}')::text as count_max,
    to_timestamp(v.heureFin, 'HH24:MI:SS')::time - to_timestamp(v.heureDebut, 'HH24:MI:SS')::time as temps_releve,
    (
        case
            when toc.data->'count_exact'::text = 'null' then 'Compté'
            else 'Estimé'
        end
    )::text as type_denombrement,
    obs.comments as commentaireObs
from sites s
    JOIN module m ON m.id_module = s.id_module
    JOIN visites v ON v.id_module = m.id_module
    JOIN gn_monitoring.t_observations obs ON obs.id_base_visit = v.id_base_visit
    JOIN gn_monitoring.t_observation_complements toc ON toc.id_observation = obs.id_observation
    JOIN taxonomie.taxref t ON t.cd_nom = obs.cd_nom
    JOIN ref_nomenclatures.t_nomenclatures tn ON (
        (toc.data->>'id_nomenclature_etat_bio'::text)::integer
    ) = tn.id_nomenclature
    JOIN ref_nomenclatures.t_nomenclatures tn1 ON (
        (toc.data->>'id_nomenclature_stade'::text)::integer
    ) = tn1.id_nomenclature
    JOIN ref_nomenclatures.t_nomenclatures tn2 ON (
        (toc.data->>'id_nomenclature_sex'::text)::integer
    ) = tn2.id_nomenclature