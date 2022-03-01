#!/bin/bash

mongo "$MONGO_INITDB_DATABASE" <<EOF
db.createUser(
   {
     user: '$MONGO_NON_ROOT_USERNAME',
     pwd: '$MONGO_NON_ROOT_PASSWORD',
     roles: [{ role: "readWrite", db: '$MONGO_INITDB_DATABASE' }]
   }
)
EOF
