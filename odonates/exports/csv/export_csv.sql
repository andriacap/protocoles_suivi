-- Vue générique pour alimenter la synthèse dans le cadre d'un protocole site-visite-observation
-- 
-- Ce fichier peut être copié dans le dossier du sous-module et renommé en synthese.sql (et au besoin personnalisé)
-- le fichier sera joué à l'installation avec la valeur de module_code qui sera attribué automatiquement
--
--
-- Personalisations possibles
--
--  - ajouter des champs specifiques qui peuvent alimenter la synthese
--      jointure avec les table de complement
--
--  - choisir les valeurs de champs de nomenclatures qui seront propres au modules
-- ce fichier contient une variable :module_code (ou :'module_code')
-- utiliser psql avec l'option -v module_code=<module_code
-- ne pas remplacer cette variable, elle est indispensable pour les scripts d'installations
-- le module pouvant être installé avec un code différent de l'original
drop view if exists gn_monitoring.v_export_odonates_standard;
create or replace view gn_monitoring.v_export_odonates_standard as with module as (
        select *
        from gn_commons.t_modules tm
        where module_code = 'odonates'
    ),
    sites as (
        select tbs.id_base_site,
            sc.id_module as id_module,
            tbs.base_site_name as nompzh,
            tbs.base_site_code,
            tbs.base_site_description,
            tbs.id_inventor,
            tbs.id_digitiser,
            coalesce (tbs.meta_update_date, first_use_date) as date_site,
            tbs.altitude_min,
            tbs.altitude_max,
            tbs.geom_local,
            st_x(ST_Centroid(geom)) as wgs84_x,
            st_y(ST_Centroid(geom)) as wgs84_y,
            st_x(ST_Centroid(geom_local)) as l93_x,
            st_y(ST_Centroid(geom_local)) as l93_y,
            (sc.data::json#>>'{pzhCode}')::text as pzhCode,
            (roles.nom_role || ' ' || roles.prenom_role) as numer_site,
            org.nom_organisme as organisme,
            ARRAY_AGG(distinct(dep.area_name)) as departement,
            ARRAY_AGG(distinct(dep.area_code)) as code_dep,
            ARRAY_AGG(distinct(com.area_name)) as commune,
            array(
                select obs
                from unnest(
                        array(
                            select jsonb_array_elements_text(sc.data->'qObserver')::text
                        )
                    ) obs
            ) as qObserver,
            array(
                select tr.nom_role || ' ' || tr.prenom_role
                from unnest(
                        array(
                            select jsonb_array_elements_text(sc.data->'observateurCompl')::int
                        )
                    ) entity_id
                    join utilisateurs.t_roles tr on tr.id_role = entity_id
            ) as observateurCompl,
            (sc.data::json#>>'{gestionnaire}')::text as gestionnairepzh,
            (sc.data::json#>>'{owner}')::text as proprietairepzh,
            (sc.data::json#>>'{proprietepzh}')::text as proprietepzh,
            -- (sc.data::json#>>'{typeZH}')::text AS typezh,
            tn2.label_fr typezh,
            tn3.label_fr methodepzh
        from gn_monitoring.t_base_sites as tbs
            join gn_monitoring.t_site_complements sc on sc.id_base_site = tbs.id_base_site -- LEFT JOIN ref_nomenclatures.t_nomenclatures tn1 ON tn1.id_nomenclature::text = (sc.data->>'typeZH')::text
            join ref_nomenclatures.t_nomenclatures tn2 on tn2.id_nomenclature::int = tbs.id_nomenclature_type_site
            join ref_nomenclatures.t_nomenclatures tn3 on tn3.id_nomenclature::text = (sc.data->>'id_nomenclature_prospection')::text
            join utilisateurs.t_roles roles on roles.id_role = tbs.id_digitiser
            join utilisateurs.bib_organismes org on org.id_organisme = roles.id_organisme
            join (
                select la.area_name,
                    csa.id_base_site
                from ref_geo.l_areas la
                    join ref_geo.bib_areas_types bat on la.id_type = bat.id_type
                    join gn_monitoring.cor_site_area csa on csa.id_area = la.id_area
                where bat.type_code = 'COM'
            ) com on tbs.id_base_site = com.id_base_site
            join (
                select la.area_name,
                    la.area_code,
                    csa.id_base_site
                from ref_geo.l_areas la
                    join ref_geo.bib_areas_types bat on la.id_type = bat.id_type
                    join gn_monitoring.cor_site_area csa on csa.id_area = la.id_area
                where bat.type_code = 'DEP'
            ) dep on tbs.id_base_site = dep.id_base_site
        group by tbs.id_base_site,
            numer_site,
            id_module,
            organisme,
            sc.data,
            typezh,
            methodepzh
    ),
    visites as (
        select tbv.id_base_site,
            tbv.id_module,
            tbv.id_base_visit,
            STRING_AGG(
                tr_digi.nom_role || ' ' || tr_digi.prenom_role,
                ', '
                order by tr_digi.nom_role,
                    tr_digi.prenom_role
            ) as numer_visit,
            string_agg(
                distinct concat (UPPER(tr.nom_role), ' ', tr.prenom_role),
                ', '
                order by concat (UPPER(tr.nom_role), ' ', tr.prenom_role)
            ) as observers_visit,
            (tvc.data::json#>>'{qObserver}')::text as qObserver_visit,
            array(
                select tr.nom_role || ' ' || tr.prenom_role
                from unnest(
                        array(
                            select jsonb_array_elements_text(tvc.data->'observateurCompl')::int
                        )
                    ) entity_id
                    join utilisateurs.t_roles tr on tr.id_role = entity_id
            ) as observateurCompl_visit,
            org.nom_organisme as organisme_numer_visit,
            tbv.visit_date_min as date_visit,
            (tvc.data::json#>>'{heureDebut}')::text as heureDebut,
            (tvc.data::json#>>'{heureFin}')::text as heureFin,
            (tvc.data::json#>>'{passage}')::text as passage,
            (tvc.data::json#>>'{periode}')::text as periode,
            (tvc.data::json#>>'{tempAir}')::text as tempAir,
            (tvc.data::json#>>'{humidite}')::text as humidite,
            (tvc.data::json#>>'{pluviosite}')::text as pluviosite,
            (tvc.data::json#>>'{couvertureNuageuse}')::text as couvertureNuageuse,
            (tvc.data::json#>>'{vent}')::text as vent,
            (tvc.data::json#>>'{pertubations}')::text as pertubations,
            (tvc.data::json#>>'{comments}')::text as commentaireVisite
        from gn_monitoring.t_base_visits tbv
            join gn_monitoring.t_visit_complements tvc on tvc.id_base_visit = tbv.id_base_visit
            join gn_monitoring.cor_visit_observer cvo on cvo.id_base_visit = tbv.id_base_visit
            join utilisateurs.t_roles tr on tr.id_role = cvo.id_role
            join utilisateurs.t_roles tr_digi on tr_digi.id_role = tbv.id_digitiser
            join utilisateurs.bib_organismes org on org.id_organisme = tr_digi.id_organisme -- WHERE
            --     DATE_PART('YEAR', tbv.visit_date_min) = DATE_PART('YEAR', current_timestamp) -1
        group by tbv.id_base_site,
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
    join module m on m.id_module = s.id_module
    join visites v on v.id_module = m.id_module
    join gn_monitoring.t_observations obs on obs.id_base_visit = v.id_base_visit
    join gn_monitoring.t_observation_complements toc on toc.id_observation = obs.id_observation
    join taxonomie.taxref t on t.cd_nom = obs.cd_nom
    join ref_nomenclatures.t_nomenclatures tn on (
        (toc.data->>'id_nomenclature_etat_bio'::text)::integer
    ) = tn.id_nomenclature
    join ref_nomenclatures.t_nomenclatures tn1 on (
        (toc.data->>'id_nomenclature_stade'::text)::integer
    ) = tn1.id_nomenclature
    join ref_nomenclatures.t_nomenclatures tn2 on (
        (toc.data->>'id_nomenclature_sex'::text)::integer
    ) = tn2.id_nomenclature