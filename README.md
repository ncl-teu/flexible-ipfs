# flexible-ipfs
## インストール
- 一式をコピーする．
- ipfsディレクトリの名前を.ipfs へ変更してください．隠しディレクトリ扱いです．
- .ipfs/configがあればconfigを削除してください．初回起動時に自動生成して，かつピアIDも自動生成してconfigに書き込まれます．
- Merkle DAG関連のデータはpropertiesファイルにあるipfs.providerspath, コンテンツ生データはipfs.datapathで指定したディレクトリに保存されます．このディレクトリ名は，kadrtt.propertiesで指定してください．
- kadrtt.propertiesのipfs.endpoint，つまりbootstrapノード情報を適切なものにしてください．/ip4/IPアドレス/tcp/4001/ipfs/ピアID　という形式です．ピアIDは，後述の方法で取得可能．
## コンパイル・実行
- Javaがインストール済みである必要があります．実行するためには，できればJava Runtime17以上が望ましいです．開発するには，JDK17以上が望ましいです
- antでコンパイルします．antが設定済み（PATH含めて）である必要があります．以下のコマンドでコンパイルしてください．
~~~
//ビルドの場合
ant
//クリーンする場合
ant clean
~~~
- もしくは，IDEで開発する場合は，以下のものをclasspathに入れてビルドすればコンパイルは通ります．
  - . (現在のトップディレクトリ)
  - \classes\production\nabu-master
  - libディレクトリ内の全てのjarファイル
- 以下のコマンドでは，一斉に実行に必要なファイル群をそれらにアップロードします．その際，peerlistファイルで，ノードのIP，sshログインユーザ名，パスワードを指定する必要があります．
~~~
./upload.sh
~~~
- 各ノードで，以下のコマンドでipfsプロセスを開始します．
~~~
./run.sh & またはrun.batをダブルクリック（windowsの場合)
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
![d9b672d92aa955025e8e5853d0e5ca63](https://github.com/ncl-teu/flexible-ipfs/assets/4952618/3bcd9b63-8ec9-414a-9f6b-b57bebce2479)

### 属性情報と各基準値のPUT
- 例えば，24時間を1時間単位で検索させたい場合は，例えばtimeという属性名で1,2,3,...24という値をputします．各時間の担当ノードが決められて，それらにputされます．
- 属性検索をする場合は，事前にこの処理が必要となります．
~~~
//以下の例は，timeという属性について，08時～10時までの値を担当ノードへputしている．各値の担当ノードはKademliaによって自動的に決められます．
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putattrs?attrname=time&min=08&max=10"
~~~
### 属性つきコンテンツのPUT
~~~
//文字列をputする場合．valueを値，attrnameは属性名, attrvalueは属性値
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?value=testdata&attrname=time&attrvalue=0825"
//ファイルをputする場合．
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?file=ファイルpath&attrname=time&attrvalue=0825"
~~~
### 1つ以上の属性について，それらの値の範囲指定によるコンテンツ検索
~~~
//timeが09~10で，かつcidのみを取得する場合（コンテンツそのものが欲しい場合は，cidonly以降を消す．）
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_09_10&cidonly=true"
//timeが08~10で，かつtemp(温度)が25～35である場合で，cidのみを取得する場合（コンテンツそのものが欲しい場合は，cidonly以降を消す．）
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_08_10-temp_25_35&cidonly=true"
~~~
### ノードが保持するDBのテーブルデータを空にする場合
- 各ノードは，h2 DBにてメタ情報を管理しています．これらを空にするには以下のコマンドを実行させてください．
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/dht/inittable"
~~~
### ipfsプロセスを終了する場合
- Ctrl+Cか，もしくは以下のコマンドでkillできます．
~~~
  curl -X POST "http://127.0.0.1:5001/api/v0/exit"
~~~
## 開発用ドキュメント
- クライアントから要求を受け付けるAPIHandler.javaと，要求元からのクエリを受けるKademliaEngine.javaがあります．

