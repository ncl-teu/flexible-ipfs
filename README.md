# flexible-ipfs
## インストール
- 一式をコピーする．
- .ipfs/configがあればconfigを削除してください．初回起動時に自動生成して，かつピアIDも自動生成してconfigに書き込まれます．
- Merkle DAG関連のデータはpropertiesファイルにあるipfs.providerpath, コンテンツ生データはipfs.datapathで指定したディレクトリに保存されます．このディレクトリ名は，kadrtt.propertiesで指定してください．
- JDKがインストール済みである必要があります．
- 以下のコマンドでは，一斉に実行に必要なファイル群をそれらにアップロードします．その際，peerlistファイルで，ノードのIP，sshログインユーザ名，パスワードを指定する必要があります．
~~~
./upload.sh
~~~
- 各ノードで，以下のコマンドでipfsプロセスを開始します．
~~~
./run.sh またはrun.batをダブルクリック（windowsの場合)
~~~
以降は，ipfsプロセスが稼働した後の操作です．ipfs-nabuでは，httpサーバが稼働しているため，ローカルにてhttpでの通信をすることによってコマンドを発行します．
### 自身のノードIDを確認する．
- ipfs起動中に以下のコマンドを実行するか，もしくは./ipfs/configファイルで確認する．
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/id"
~~~
### コンテンツをputする．
- PUT対象のデータのCIDを取得し，あとはKademliaに従ってPUT先を決めて保存させる．
- putされたコンテンツは，propertiesファイル内で定義されているipfs.datapathの場所に保存される．
~~~
//文字列をPUTする場合
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?value=文字列"
//ファイルをPUTする場合
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?file=ファイルパス"
~~~
### コンテンツをgetする．
- GET対象のコンテンツのCIDを指定することで，コンテンツをgetできる．
- getしたコンテンツは，propertiesファイル内で定義されているipfs.datapathの場所に保存される．
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getvalue?cid=対象コンテンツのCID"
~~~
