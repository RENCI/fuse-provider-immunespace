import json
import os
import re
from io import BytesIO

import numpy
import pycurl
import requests
from labkey.api_wrapper import APIWrapper
from labkey.query import QueryFilter

api = APIWrapper(domain="www.immunespace.org", container_path="Studies", use_ssl=True, api_key="apikey|01a141db71869525cbf60a5a333edd31", disable_csrf=True)


def curl_get_into_file(url: str, http_header: list):
    data_raw = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(crl.HTTPHEADER, http_header)
    crl.setopt(crl.URL, url)
    crl.setopt(crl.WRITEDATA, data_raw)
    crl.perform()
    crl.close()
    data = data_raw.getvalue().decode('utf8')
    return data

# result = api.query.select_rows(schema_name="assay.ExpressionMatrix.matrix", query_name="SelectedRuns", view_name="expression_matrices", timeout=30)
# print(result)


def get_feature_annotation_map(feature_set_id: str):
    feature_annotation_set_results = api.query.select_rows(schema_name="Microarray", query_name="FeatureAnnotationSet", timeout=30,
                                                           filter_array=[QueryFilter("RowId", feature_set_id, QueryFilter.Types.EQUAL)])
    feature_annotation_set_rows = feature_annotation_set_results["rows"]
    print("feature_annotation_set_rows: %s" % feature_annotation_set_rows)
    feature_annotation_set_name = re.sub("_orig", "", feature_annotation_set_rows[0]["Name"])
    print("feature_annotation_set_name: %s" % feature_annotation_set_name)

    fas_map_results = api.query.select_rows(schema_name="Microarray", query_name="FasMap", timeout=30,
                                            filter_array=[QueryFilter("Name", feature_annotation_set_name, QueryFilter.Types.EQUAL)])
    fas_map_rows = fas_map_results["rows"]
    print("fas_map_rows: %s" % fas_map_rows)

    fas_map_curr_id = fas_map_rows[0]["currId"]
    fas_map_orig_id = fas_map_rows[0]["origId"]

    feature_annotation_query = f"SELECT FeatureId, GeneSymbol from FeatureAnnotation where FeatureAnnotationSetId='{fas_map_curr_id}';"
    feature_annotation_results = api.query.execute_sql(schema_name="Microarray", sql=feature_annotation_query, timeout=30)

    feature_annotation_map = list(map(lambda feature_annotation_row: (feature_annotation_row["FeatureId"], feature_annotation_row["GeneSymbol"]), feature_annotation_results["rows"]))

    # map_file = open("/tmp/feature_annotation_map.txt", "r+")
    # [map_file.write(f"{line}\n") for line in feature_annotation_map]
    # map_file.close()

    #print("feature_annotation_map: %s" % feature_annotation_map)

    return dict(feature_annotation_map)


def main():
    print("Create a server context")

    participant_group_search_response = requests.get(
        url="https://www.immunespace.org/participant-group/Studies/browseParticipantGroups.api?/distinctCatgories=false&type=participantGroup&includeUnassigned=false&includeParticipantIds=false",
        headers={'accept': 'application/json', 'apikey': 'apikey|5d2f826c452af1849b3f106630fef50a'})

    participant_group_search_response_json = participant_group_search_response.json()
    # participant_group_search_response = curl_get_into_json(
    #     'https://www.immunespace.org/participant-group/Studies/browseParticipantGroups.api?/distinctCatgories=false&type=participantGroup&includeUnassigned=false&includeParticipantIds=false',
    #     ['apikey: apikey|5d2f826c452af1849b3f106630fef50a'])

    # print('participant_group_search_response: \n%s' % participant_group_search_response)

    # participant_group_id = "zxcv"
    participant_group_id = "asdf"
    participant_ids = {}
    unique_participants = {}
    study_members = {}

    for a in participant_group_search_response_json["groups"]:
        if a["label"] != participant_group_id:
            continue
        participant_ids = a["category"]["participantIds"]
        study_members = json.loads(a["filters"])["Study"]["Study"]["members"]
        # unique_participants = list(map(lambda st: str.replace(st, "SUB", ""), participant_ids))
        # print('unique_participants: %s' % unique_participants)
        # print('id: %s, group_name: %s, created: %s, # of subjects: %s, # of studies: %s' % (a["id"], a["label"], a["created"], len(participant_ids), len(set(unique_participants))))

    print('participant_group_id: %s, study_members: %s, participant_ids : %s' % (participant_group_id, study_members, participant_ids))
    # print('unique_participants: %s' % unique_participants)

    sql = "SELECT DISTINCT Run.Name run_name FROM InputSamples_computed WHERE Biosample.participantId IN ('%s')" % "','".join(participant_ids)
    # print('sql: %s' % sql)
    run_name_results = api.query.execute_sql("assay.ExpressionMatrix.matrix", sql, container_filter="CurrentAndSubfolders", timeout=30)
    run_name_rows = run_name_results["rows"]
    # print("run_name_rows: %s" % run_name_rows)
    run_names = list(map(lambda run_name_row: str(run_name_row["run_name"]), run_name_rows))
    print("run_names: %s" % run_names)

    selected_runs_results = api.query.select_rows("assay.ExpressionMatrix.matrix", query_name="SelectedRuns", view_name="expression_matrices",
                                                  filter_array=[QueryFilter("Folder/Name", ";".join(study_members), QueryFilter.Types.IN)])

    print(selected_runs_results["rows"])

    biosample_accessions = list()
    gene_expressions = dict()

    for r in selected_runs_results["rows"]:

        run_id = r["RowId"]
        feature_set_id = r["featureSet"]
        feature_annotation_map = get_feature_annotation_map(feature_set_id)
        download_link = r["download_link"]

        print("run_id: %s, feature_set_id: %s, download_link: %s" % (run_id, feature_set_id, download_link))

        labkey_download_link = "https://www.immunespace.org%s" % r["_labkeyurl_download_link"]
        download_data = curl_get_into_file(labkey_download_link, ['apikey: apikey|5d2f826c452af1849b3f106630fef50a'])

        features = {}
        missing_features = list()
        gene_names = set()

        # gene_expression_filtered_csv_path = os.path.join('/tmp', download_link.replace("tsv", "filtered.csv"))

        lines = []

        for idx, line in enumerate(download_data.split("\n")):

            if idx == 0:
                features = line.replace("feature_id\t", "").replace("\t", ",").replace("\n", "").split(",")
                continue

            comma_delimited_line = line.replace("\t", ",").replace("\n", "")
            feature_value = comma_delimited_line.split(",")[0]

            if not feature_annotation_map.__contains__(feature_value):
                # print("does not exist: %s" % feature_value)
                missing_features.append(feature_value)
                continue

            if feature_annotation_map.get(feature_value) is None:
                continue

            new_feature_value = feature_annotation_map.get(feature_value)
            gene_names.add(new_feature_value)
            # print("feature_value: %s, new_feature_value: %s" % (feature_value, new_feature_value))
            fixed_line = comma_delimited_line.replace(feature_value, new_feature_value)
            lines.append(fixed_line)
            # csv_file.write(f"{fixed_line}\n")

        # csv_file.close()

        print("len(lines): %s" % len(lines))
        gene_names = sorted(gene_names)
        print("len(gene_names): %s" % len(gene_names))
        gene_expression_csv_path = os.path.join('/tmp', download_link.replace("tsv", "csv"))
        print("writing %s " % gene_expression_csv_path)

        with open(gene_expression_csv_path, "w") as csv_file:
            for (idx, gene_name) in enumerate(gene_names):
                relevant_lines = list(filter(lambda x: x.split(",")[0] == gene_name, lines))
                if len(relevant_lines) > 1:
                    tmp = list(map(lambda a: list(map(lambda b: float(b), a.split(",")[1:])), relevant_lines))
                    mean = numpy.mean(numpy.array(tmp), axis=0)
                    values = ",".join(list(map(lambda x: "{:.10f}".format(x), mean.tolist())))
                else:
                    values = ",".join(relevant_lines[0].split(",")[1:])
                merged_line = f"{gene_name},{values}"
                # print("merged_line: %s" % merged_line)
                # lines_merged_by_mean.append(merged_line)
                csv_file.write("%s\n" % merged_line)
                print("finished: %s/%s" % (idx, len(gene_names)))
        csv_file.close()

    print("biosample_accessions: %s" % biosample_accessions)
    print("gene_expressions: %s" % gene_expressions)

    # missing_features = list()
    #
    # with open(gene_expression_tsv_path, "r") as tsv_file:
    #     header_line = tsv_file.readline()
    #     cleaned_header_line = header_line.replace("feature_id\t", "").replace("\t", ",").replace("\n", "")
    #     print("cleaned_header_line: %s" % cleaned_header_line)
    #     for x in cleaned_header_line.split(","):
    #         biosample_accessions.append(x)
    #
    #     for line in tsv_file:
    #         cleaned_line = line.replace("\t", ",").replace("\n", "")
    #         cleaned_line_split = cleaned_line.split(",")
    #         feature_id = cleaned_line_split[0]
    #
    #         if not feature_annotation_map.__contains__(feature_id):
    #             # print("does not exist: %s" % feature_value)
    #             missing_features.append(feature_id)
    #             continue
    #
    #         if feature_annotation_map.get(feature_id) is None:
    #             continue
    #
    #         gene_name = feature_annotation_map.get(feature_id)
    #
    #         if not gene_expressions.__contains__(feature_id):
    #             gene_expressions[feature_id] = list()
    #         else:
    #             gene_expressions[feature_id].extend(cleaned_line_split[1:])
    #
    # tsv_file.close()

    # runs_results = api.query.select_rows("assay.ExpressionMatrix.matrix", query_name="Runs", view_name="expression_matrices",
    #                                      filter_array=[QueryFilter("RowId", run_id, QueryFilter.Types.EQUAL)])
    # print("runs_results: %s" % runs_results["rows"])

    # features = {}
    # missing_features = list()
    # gene_names = set()
    # with open(gene_expression_tsv_path, "r") as tsv_file:
    #     with open(gene_expression_filtered_csv_path, "w") as csv_file:
    #         for idx, line in enumerate(tsv_file):
    #             if idx == 0:
    #                 features = re.sub("\t", ",", line.replace("feature_id\t", "").replace("\n", "")).split(",")
    #                 continue
    #             comma_delimited_line = re.sub("\t", ",", line).replace("\n", "")
    #             feature_value = comma_delimited_line.split(",")[0]
    #
    #             if not feature_annotation_map.__contains__(feature_value):
    #                 # print("does not exist: %s" % feature_value)
    #                 missing_features.append(feature_value)
    #                 continue
    #
    #             if feature_annotation_map.get(feature_value) is None:
    #                 continue
    #
    #             new_feature_value = feature_annotation_map.get(feature_value)
    #             gene_names.add(new_feature_value)
    #             print("feature_value: %s, new_feature_value: %s" % (feature_value, new_feature_value))
    #             csv_file.write(f"{comma_delimited_line.replace(feature_value, new_feature_value)}\n")
    #     csv_file.close()
    # tsv_file.close()
    #
    # # print("missing_features: %s" % missing_features)
    #
    # print("features.len: %s" % len(features))
    # os.remove(gene_expression_tsv_path)
    #
    # lines = []
    # with open(gene_expression_filtered_csv_path, "r") as csv_file:
    #     reader = csv.reader(csv_file)
    #     for line in reader:
    #         lines.append(line)
    # csv_file.close()
    #
    # gene_names = sorted(gene_names)
    #
    # gene_expression_csv_path = os.path.join('/tmp', 'geneBySampleMatrix.csv')
    # with open(gene_expression_csv_path, "w") as csv_file:
    #     for gene_name in gene_names:
    #         asdf = []
    #         for line in lines:
    #             key = line[0]
    #             if key != gene_name:
    #                 continue
    #             data = list(map(lambda x: float(x), line[1:]))
    #             asdf.append(data)
    #         np_array = numpy.array(asdf)
    #         mean = numpy.mean(np_array, axis=0)
    #         print("%s: %s" % (gene_name, mean))
    #         csv_file.write("%s,%s\n" % (gene_name, ",".join(list(map(lambda x: "{:.13f}".format(x), mean.tolist())))))
    # csv_file.close()
    #
    # # for gene_name in gene_names:
    # #     asdf = []
    # #     with open(gene_expression_filtered_csv_path, "r") as csv_file:
    # #         for line in csv_file:
    # #             line_split = line.split(",")
    # #             key = line_split[0]
    # #             if key != gene_name:
    # #                 continue
    # #             data = list(map(lambda x: float(x.replace("\n", "")), line_split[1:]))
    # #             asdf.append(data)
    # #     csv_file.close()
    # #     np_array = numpy.array(asdf)
    # #     mean = numpy.mean(np_array, axis=0)
    # #     print("%s: %s" % (gene_name, mean))
    #
    # phenotype_csv_path = os.path.join('/tmp', "phenoDataMatrix.csv")
    # features = list(filter(lambda x: x.startswith('BS'), features))
    # filters = [
    #     QueryFilter("Run", run_id, QueryFilter.Types.EQUAL),
    #     QueryFilter("biosample_accession", ";".join(features), QueryFilter.Types.IN),
    # ]
    #
    # result = api.query.select_rows(schema_name="study", query_name="HM_inputSmplsPlusImmEx", container_filter="CurrentAndSubfolders", filter_array=filters)
    #
    # with open(phenotype_csv_path, "w") as file_output:
    #     file_output.write(
    #         "participant_id,study_time_collected,study_time_collected_unit,cohort,cohort_type,biosample_accession,exposure_material_reported,exposure_process_preferred\n")
    #     for row in result["rows"]:
    #         # print("row: %s" % row)
    #         csv_record = f'{row["ParticipantId"]},{row["study_time_collected"]},{row["study_time_collected_unit"]},{row["cohort"]},{row["cohort_type"]},{row["biosample_accession"]},{row["exposure_material_reported"][0]},{row["exposure_process_preferred"][0]}'
    #         # print("csv_record: %s" % csv_record)
    #         file_output.write(f"{csv_record}\n")
    # file_output.close()


if __name__ == "__main__":
    main()
