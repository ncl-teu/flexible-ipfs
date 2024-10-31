# flexible-ipfs
# bootstrapノードとして実行する手順
- ./run.sh & またはrun.batをダブルクリック（windowsの場合)
- .ipfs/configが生成され，自身のPeerIDとPrivate Keyが書き込まれる．
- 一旦，CTRL+CにてIPFSを終了させる．
- kadrtt.propertiesのipfs.endpointを以下のようにする．
~~~
ipfs.endpoint=/ip4/[bootstrapのIP]/tcp/4001/ipfs/[bootstrapのPeerID]
例: ipfs.endpoint=/ip4/1.2.3.4/tcp/4001/ipfs/abcdedfta
~~~
- .ipfs/config内のBootstrapの部分を，以下のようにする．
~~~
	"Bootstrap":[
		"/ip4/[bootstrapのIP]/tcp/4001/ipfs/[bootstrapのPeerID]"
	],
  例:
	"Bootstrap":[
		"/ip4/1.2.3.4/tcp/4001/ipfs/abcdedfta"
	],
~~~
- そして再び，./run.sh & またはrun.batをダブルクリック（windowsの場合)して実行する．
# 一般ノードとして実行する手順
-.ipfsディレクトリに，configファイルが無いことを確認（もしあれば削除する）
- kadrtt.propertiesのipfs.endpointを以下のようにする．
~~~
ipfs.endpoint=/ip4/[bootstrapのIP]/tcp/4001/ipfs/[bootstrapのPeerID]
例: ipfs.endpoint=/ip4/1.2.3.4/tcp/4001/ipfs/abcdedfta
~~~
- そして，以下のコマンドでipfsプロセスを開始します．
~~~
./run.sh & またはrun.batをダブルクリック（windowsの場合)
~~~
以降は，ipfsプロセスが稼働した後の操作です．ipfs-nabuでは，httpサーバが稼働しているため，ローカルにてhttpでの通信をすることによってコマンドを発行します．
### 自身のノードIDを確認する．
- ipfs起動中に以下のコマンドを実行するか，もしくは./ipfs/configファイルで確認する．
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/id"
~~~
### コンテンツをaddする．
- 自身のコンテンツをチャンクに分割し，公開用フォルダに複製します．その後，チャンクのCIDに最も近いノードへ各チャンクをputします．
- コンテンツをチャンクへ分割する
- 複数のチャンク＋1つのメタデータ(MerkleDAG)が生成される．MerkleDAGには，全チャンクのCIDが書き込まれる．
- MerkleDAGは，**ipfs.providers**フォルダへ保存される．
- 全チャンクは，**ipfs.getdata**フォルダへ保存される．
- まずは，MerkleDAGは，Hash(コンテンツ）のCIDでput先をKademliaで探す．
- もしHash(コンテンツ)とのXOR距離が最小のものがノードBであれば，MerkleDAGをノードBへPUTする．
- その後，チャンクごとにPUT先を決めて，非同期でそれぞれPUTする．
  - 非同期PUT用のスレッド数は**ipfs.chunkputthreads**パラメータで設定可能．
- **ipfs.providers**にはMerkleDAG**, ipfs.getdata**にはチャンクがPUTされる．
~~~
//Addコマンド
curl -X POST "http://127.0.0.1:5001/api/v0/dht/add?file=xxx"
//属性付きコンテンツのAdd．書式: attrs=属性名1_属性値1-属性名2_属性値2-....
curl -X POST "http://127.0.0.1:5001/api/v0/dht/add?file=xxxx&attrs=time_0824"
//Tag付きコンテンツのAdd (値を文字列とする場合にtagsを使う）．書式: tags=tag名1_tag値1-tag名2_tag値2-...
curl -X POST "http://127.0.0.1:5001/api/v0/dht/add?file=xxxx&tags=location_tokyo"
//複数属性と複数Tag付きコンテンツのAdd (値を文字列とする場合にtagsを使う）
curl -X POST "http://127.0.0.1:5001/api/v0/dht/add?file=xxxx&attrs=time_20240323-temp_34&tags=area_fe43-id_345t"
//実行結果の出力: 
{"CID_file":"addしたコンテンツのCID","Addr0":"12D3KooWRwtcPecyqCLkzwwP3xSVo8XtuVDSQMkoi7NJc2sjRTjR: [/ip4/xx.xx.xx.xx/tcp/4001]"}

//Getコマンド:
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getvalue?cid=addしたコンテンツのCID"
~~~
### コンテンツをputする．
- PUT対象のデータのCIDを取得し，あとはKademliaに従ってPUT先ノードが決まり，そのノードへ保存させる．
- putされたコンテンツは，propertiesファイル内で定義されているipfs.datapathのpathへ保存される．
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
//以下の例は，timeという属性について，08時～10時までの値を担当ノードへputしている．各値の担当ノードはKademliaによって自動的に決められます．書式: attrname=属性名&min=最小値&max=最大値
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putattrs?attrname=time&min=08&max=10"
~~~
### 属性つきコンテンツのPUT
~~~
//1属性で文字列をputする場合．属性の書式は，attrs=KEY_VALUE-KEY_VALUE-....
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?value=xxxx&attrs=time_0824"
//1属性でファイルをputする場合．
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?file=xxxx&attrs=time_0824"
//複数属性と複数Tagつきコンテンツput
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?file=xxxx&attrs=time_20240323-temp_34&tags=area_fe43-id_345t"

//文字列をputする場合．属性の書式は，attrs=KEY_VALUE-KEY_VALUE-....
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?value=xxxx&attrs=time_0824-temp_25"
//ファイルをputする場合．
curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?file=xxxx&attrs=time_0824-temp_25"
~~~
### 1つ以上の属性について，それらの値の範囲指定によるコンテンツ検索
~~~
//timeが08~10で，かつcidのみを取得する場合
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_08_10"
//timeが08~10で，かつtemp(温度)が25～35である場合で，cidのみを取得する場合
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_08_10-temp_25_35"
~~~
### IPFSに保持されている属性名一覧の取得
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/dht/listattrs"
~~~
### 1つ以上のTagについて，Tag値によるコンテンツ検索
~~~
//tag名前をareaで，値を指定して取得する場合
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?tags=area_a23fa"
//複数属性範囲と複数タグでの取得
curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_08_10-temp_25_35&tags=area_a23fa-id_234fs"
~~~
### IPFSに保持されているTag名一覧の取得
~~~
curl -X POST "http://127.0.0.1:5001/api/v0/dht/listtags"
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
## インストール
- 一式をコピーする．
- .ipfs/configがあればconfigを削除してください．初回起動時に自動生成して，かつピアIDも自動生成してconfigに書き込まれます．
- Merkle DAG関連のデータはpropertiesファイルにあるipfs.providerspath, コンテンツ生データはipfs.datapathで指定したディレクトリに保存されます．このディレクトリ名は，kadrtt.propertiesで指定してください．
- kadrtt.propertiesのipfs.endpoint，つまりbootstrapノード情報を適切なものにしてください．/ip4/IPアドレス/tcp/4001/ipfs/ピアID　という形式です．ピアIDは，後述の方法で取得可能．
## コンパイル・実行
- Javaがインストール済みである必要があります．実行するためには，できればJava Runtime17以上が望ましいです．開発するには，JDK17以上が望ましいです
- antでコンパイルします．antが設定済み（PATH含めて）である必要があります．以下のコマンドでコンパイルしてください．
~~~
//コンパイル
ant compile
//classファイル→lib/ipfs-ncl.jarを生成
ant build
//クリーンする場合
ant clean
~~~
- もしくは，IDEで開発する場合は，以下のものをclasspathに入れてビルドすればコンパイルは通ります．
  - . (現在のトップディレクトリ)
  - \classes\production\nabu-master
  - libディレクトリ内の全てのjarファイル
- 以下のコマンドでは，一斉に実行に必要なファイル群をそれらにアップロードします．その際，peerlistファイルで，ノードのIP，sshログインユーザ名，パスワードを指定する必要があります．
- classes/, ./にある設定ファイル群，lib/をuploadします．
~~~
./upload.sh
~~~

