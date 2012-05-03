var http = require('http');

http.createServer(function(req,res){
  res.statusCode = 404;
  res.setHeader('content-type','text/plain')
  res.setHeader('content-length',3)
  res.setHeader('server','MochiWeb/1.1 WebMachine/1.9.0 (someone had painted it blue)')
  res.setHeader('date','Thu, 03 May 2012 17:33:11 GMT')
  

  // test if connection: keep-alive is the culprit by removing it from the 
  // response header
  // res.writeHead(404)
  // res._header = res._header.replace(/Connection: keep-alive\r\n/,'')

  res.end('ok!')
}).listen(1234,'localhost')

function del(key){
  var req = http.request({
    host: 'localhost',
    // port: 1234,
    port: 8098,
    method: 'delete',
    path: '/riak/test-keys/'+key,
    headers: { 'User-Agent': 'riak-js', 'content-length': 0 }
  })

  req.on('response',function(res){
    // console.log(res)
    var host = 'localhost:1234'
      , reqs = req.agent.requests && req.agent.requests[host] && req.agent.requests[host].length
      , socs = req.agent.sockets && req.agent.sockets[host] && req.agent.sockets[host].length;
    console.log('Agent(host=%s, requests=%d, sockets=%d)',host,reqs||0,socs||0)

    if( res.statusCode === 404 )
      console.log('not found');

    else if( res.statusCode === 204 || res.statusCode == 200 )
      console.log('removed',key);

    else
      console.error('failed',res.statusCode)

  })

  req.on('close',function(){
    console.error('http request closed')
  })

  req.on('socket',function(sock){
    console.log('http request socket')
    sock.on('close',function(){
      console.log('http request socket close')
    })
  })

  // throw instead when failing...
  req.on('error',function(e){
    console.error('http request error',e.stack)
  })

  req.end()
}

// calling on riak!
for( var i=0; i<7; i++ )
  del(i);