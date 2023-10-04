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
DROP view IF EXISTS gn_monitoring.v_export_odonates_standard;
CREATE OR REPLACE VIEW gn_monitoring.v_export_odonates_standard
AS WITH module AS (
         SELECT tm.id_module,
            tm.module_code,
            tm.module_label,
            tm.module_picto,
            tm.module_desc,
            tm.module_group,
            tm.module_path,
            tm.module_external_url,
            tm.module_target,
            tm.module_comment,
            tm.active_frontend,
            tm.active_backend,
            tm.module_doc_url,
            tm.module_order,
            tm.type,
            tm.meta_create_date,
            tm.meta_update_date,
            tm.ng_module
           FROM gn_commons.t_modules tm
          WHERE tm.module_code::text = 'odonates'::text
        ), sites AS (
         SELECT tbs.id_base_site,
            sc.id_module,
            tbs.base_site_name AS nompzh,
            tbs.base_site_code,
            tbs.base_site_description,
            tbs.id_inventor,
            tbs.id_digitiser,
            COALESCE(tbs.meta_update_date, tbs.first_use_date::timestamp without time zone) AS date_site,
            tbs.altitude_min,
            tbs.altitude_max,
            tbs.geom_local,
            st_x(st_centroid(tbs.geom)) AS wgs84_x,
            st_y(st_centroid(tbs.geom)) AS wgs84_y,
            st_x(st_centroid(tbs.geom_local)) AS l93_x,
            st_y(st_centroid(tbs.geom_local)) AS l93_y,
            sc.data::json #>> '{pzhCode}'::text[] AS pzhcode,
            (roles.nom_role::text || ' '::text) || roles.prenom_role::text AS numer_site,
            org.nom_organisme AS organisme,
            string_agg(btrim(dep.area_name::text, '"'::text), ', '::text) AS departement,
            string_agg(btrim(dep.area_code::text, '"'::text), ', '::text) AS code_dep,
            string_agg(btrim(com.area_name::text, '"'::text), ', '::text) AS commune,
            ( SELECT string_agg(btrim(jsonb_array_elements_text.value, '"'::text), ', '::text) AS string_agg
                   FROM jsonb_array_elements_text(sc.data -> 'qObserver'::text) jsonb_array_elements_text(value)) AS qobserver,
            array_to_string(ARRAY( SELECT (tr.nom_role::text || ' '::text) || tr.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(sc.data -> 'observateurCompl'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr ON tr.id_role = entity_id.entity_id), ', '::text) AS observateurcompl,
            sc.data::json #>> '{gestionnaire}'::text[] AS gestionnairepzh,
            sc.data::json #>> '{owner}'::text[] AS proprietairepzh,
            sc.data::json #>> '{proprietepzh}'::text[] AS proprietepzh,
            tn2_1.label_fr AS typezh,
            tn3.label_fr AS methodepzh
           FROM gn_monitoring.t_base_sites tbs
             JOIN gn_monitoring.t_site_complements sc ON sc.id_base_site = tbs.id_base_site
             JOIN ref_nomenclatures.t_nomenclatures tn2_1 ON tn2_1.id_nomenclature = tbs.id_nomenclature_type_site
             JOIN ref_nomenclatures.t_nomenclatures tn3 ON tn3.id_nomenclature::text = (sc.data ->> 'id_nomenclature_prospection'::text)
             JOIN utilisateurs.t_roles roles ON roles.id_role = tbs.id_digitiser
             JOIN utilisateurs.bib_organismes org ON org.id_organisme = roles.id_organisme
             JOIN ( SELECT la.area_name,
                    csa.id_base_site
                   FROM ref_geo.l_areas la
                     JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
                     JOIN gn_monitoring.cor_site_area csa ON csa.id_area = la.id_area
                  WHERE bat.type_code::text = 'COM'::text) com ON tbs.id_base_site = com.id_base_site
             JOIN ( SELECT la.area_name,
                    la.area_code,
                    csa.id_base_site
                   FROM ref_geo.l_areas la
                     JOIN ref_geo.bib_areas_types bat ON la.id_type = bat.id_type
                     JOIN gn_monitoring.cor_site_area csa ON csa.id_area = la.id_area
                  WHERE bat.type_code::text = 'DEP'::text) dep ON tbs.id_base_site = dep.id_base_site
          GROUP BY tbs.id_base_site, ((roles.nom_role::text || ' '::text) || roles.prenom_role::text), sc.id_module, org.nom_organisme, sc.data, tn2_1.label_fr, tn3.label_fr
        ), visites AS (
         SELECT tbv.id_base_site,
            tbv.id_module,
            tbv.id_base_visit,
            string_agg((tr_digi.nom_role::text || ' '::text) || tr_digi.prenom_role::text, ', '::text ORDER BY tr_digi.nom_role, tr_digi.prenom_role) AS numer_visit,
            string_agg(DISTINCT concat(upper(tr.nom_role::text), ' ', tr.prenom_role), ', '::text ORDER BY (concat(upper(tr.nom_role::text), ' ', tr.prenom_role))) AS observers_visit,
            ( SELECT string_agg(btrim(jsonb_array_elements_text.value, '"'::text), ', '::text) AS string_agg
                   FROM jsonb_array_elements_text(tvc.data -> 'qObserver'::text) jsonb_array_elements_text(value)) AS qobserver_visit,
            array_to_string(ARRAY( SELECT (tr_1.nom_role::text || ' '::text) || tr_1.prenom_role::text
                   FROM unnest(ARRAY( SELECT jsonb_array_elements_text(tvc.data -> 'observateurCompl'::text)::integer AS jsonb_array_elements_text)) entity_id(entity_id)
                     JOIN utilisateurs.t_roles tr_1 ON tr_1.id_role = entity_id.entity_id), ','::text) AS observateurcompl_visit,
            org.nom_organisme AS organisme_numer_visit,
            tbv.visit_date_min AS date_visit,
            tvc.data::json #>> '{heureDebut}'::text[] AS heuredebut,
            tvc.data::json #>> '{heureFin}'::text[] AS heurefin,
            tvc.data::json #>> '{passage}'::text[] AS passage,
            tvc.data::json #>> '{periode}'::text[] AS periode,
            tvc.data::json #>> '{tempAir}'::text[] AS tempair,
            tvc.data::json #>> '{humidite}'::text[] AS humidite,
            tvc.data::json #>> '{pluviosite}'::text[] AS pluviosite,
            tvc.data::json #>> '{couvertureNuageuse}'::text[] AS couverturenuageuse,
            tvc.data::json #>> '{vent}'::text[] AS vent,
            tvc.data::json #>> '{pertubations}'::text[] AS pertubations,
            tvc.data::json #>> '{comments}'::text[] AS commentairevisite
           FROM gn_monitoring.t_base_visits tbv
             JOIN gn_monitoring.t_visit_complements tvc ON tvc.id_base_visit = tbv.id_base_visit
             JOIN gn_monitoring.cor_visit_observer cvo ON cvo.id_base_visit = tbv.id_base_visit
             JOIN utilisateurs.t_roles tr ON tr.id_role = cvo.id_role
             JOIN utilisateurs.t_roles tr_digi ON tr_digi.id_role = tbv.id_digitiser
             JOIN utilisateurs.bib_organismes org ON org.id_organisme = tr_digi.id_organisme
          GROUP BY tbv.id_base_site, tbv.id_base_visit, tvc.data, org.nom_organisme
        )
 SELECT s.numer_site,
    s.organisme,
    s.departement,
    s.code_dep,
    s.commune,
    s.l93_x AS longitude,
    s.l93_y AS latitude,
    s.date_site,
    s.qobserver,
    s.proprietairepzh,
    s.proprietepzh,
    s.observateurcompl,
    s.gestionnairepzh,
    s.pzhcode,
    s.typezh,
    s.methodepzh,
    v.numer_visit,
    v.observers_visit,
    v.qobserver_visit,
    v.observateurcompl_visit,
    v.organisme_numer_visit,
    v.date_visit,
    v.heuredebut,
    v.heurefin,
    v.passage,
    v.periode,
    v.tempair,
    v.humidite,
    v.pluviosite,
    v.couverturenuageuse,
    v.vent,
    v.pertubations,
    v.commentairevisite,
    t.lb_nom AS nomscientifiqueref,
    t.cd_nom,
    t.regne,
    t.classe,
    t.ordre,
    t.famille,
    t.sous_famille AS genre,
    t.nom_complet AS nomcite,
    t.nom_vern AS nomvernaculaire,
    tn.label_fr AS ocetatbio,
    tn1.label_fr AS ocstade,
    tn2.label_fr AS ocsexe,
    toc.data::json #>> '{count_exact}'::text[] AS countexact,
    toc.data::json #>> '{count_average}'::text[] AS count_average,
    toc.data::json #>> '{count_min}'::text[] AS count_min,
    toc.data::json #>> '{count_max}'::text[] AS count_max,
    to_timestamp(v.heurefin, 'HH24:MI:SS'::text)::time without time zone - to_timestamp(v.heuredebut, 'HH24:MI:SS'::text)::time without time zone AS temps_releve,
        CASE
            WHEN (toc.data -> 'count_exact'::text) = 'null'::jsonb THEN 'Compté'::text
            ELSE 'Estimé'::text
        END AS type_denombrement,
    obs.comments AS commentaireobs
   FROM sites s
     JOIN module m ON m.id_module = s.id_module
     JOIN visites v ON v.id_module = m.id_module
     JOIN gn_monitoring.t_observations obs ON obs.id_base_visit = v.id_base_visit
     JOIN gn_monitoring.t_observation_complements toc ON toc.id_observation = obs.id_observation
     JOIN taxonomie.taxref t ON t.cd_nom = obs.cd_nom
     JOIN ref_nomenclatures.t_nomenclatures tn ON ((toc.data ->> 'id_nomenclature_etat_bio'::text)::integer) = tn.id_nomenclature
     JOIN ref_nomenclatures.t_nomenclatures tn1 ON ((toc.data ->> 'id_nomenclature_stade'::text)::integer) = tn1.id_nomenclature
     JOIN ref_nomenclatures.t_nomenclatures tn2 ON ((toc.data ->> 'id_nomenclature_sex'::text)::integer) = tn2.id_nomenclature;