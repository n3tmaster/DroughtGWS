#!/bin/bash

#north=03&south=05&east=19&west=17
ILCOMANDO="curl -X GET https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_update_lst/MOD11A2/6/04/05/19/18/70599C0A-4757-11EA-9146-9AB2B99D347E"

eval $ILCOMANDO

echo "done"