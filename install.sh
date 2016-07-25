INSTALL_DIR=$(pwd)

cd postgresql-9.5.2/
./configure --prefix=$INSTALL_DIR/tmp_install --without-readline --enable-debug --enable-cassert --enable-thread-safety
make 
make install
cd ../

mkdir ./tmp_install/db
./tmp_install/bin/initdb -D ./tmp_install/db/
./tmp_install/bin/postgres -D ./tmp_install/db/ &
sleep 5
sh db_install.sh
kill -9 %
# install extension
sh ext_install.sh

cd external_sample
make
