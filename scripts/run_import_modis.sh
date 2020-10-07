#!/bin/bash



ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/04/18/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/05/18/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/04/19/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/05/19/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/03/17/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/04/17/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/05/17/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/03/18/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_evi2/MOD13Q1/6/03/19/0/0/12/70599C0A-4757-11EA-9146-9AB2B99D347E"
eval $ILCOMANDO

#ILCOMANDO="curl-X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_calculate_mc_vci/12"
#eval $ILCOMANDO

#ILCOMANDO="curl-X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_calculate_mc_evci/12"
#eval $ILCOMANDO
#ILCOMANDO="curl-X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_calculate_mc_vhi/12"
#eval $ILCOMANDO
echo "done"

