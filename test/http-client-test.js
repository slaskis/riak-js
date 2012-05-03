var Client = require('../lib/http-client')
  , Meta = require('../lib/http-meta')
  , util = require('util')
  , assert = require('assert');

describe('http client',function(){

  var db = new Client()
    , bucket;

  it('.save(bucket,key,data,{returnbody:true},fn)',function(done){
    db.save('test-users','test+returnbody@email.com',{email:'text@email.com', name:'John Doe', a:[1,2]},{returnbody: true},function(err,data,meta){
      meta.should.have.status(200)
      data.should.exist
      data.a.should.eql([1,2])
      meta.key.should.equal('test+returnbody@email.com')
      done(err)
    })
  })

  it('.save(bucket,key,data,fn)',function(done){
    db.save('test-users','test+text@email.com','Some text',function(err,data,meta){
      meta.should.have.status(204)
      meta.key.should.equal('test+text@email.com')
      assert(data === undefined)
      done(err)
    })
  })

  it('.get(bucket,key,fn)',function(done){
    db.get('test-users','test+text@email.com',function(err,data,meta){
      meta.should.have.status(200)
      data.should.equal('Some text')
      done(err)
    })
  })

  it('.getAll(bucket,fn)',function(done){
    db.getAll('test-users',function(err,users,meta){
      meta.should.have.status(200)
      users.should.be.instanceof(Array)
      users.should.include('Some text')
      done(err)
    })
  })

  it('.head(bucket,key,fn)',function(done){
    db.head('test-users','test+text@email.com',function(err,data,meta){
      meta.should.have.status(200)
      assert(data === undefined)
      done(err)
    })
  })

  it('.exists(bucket,key,fn)',function(done){
    db.exists('test-users','test+text@email.com',function(err,exists,meta){
      meta.should.have.status(200)
      exists.should.equal(true)
      done(err)
    })
  })

  it('.remove(bucket,key,fn)',function(done){
    db.remove('test-users','test+text@email.com',function(err,data,meta){
      meta.should.have.status(204)
      assert(data === undefined)
      done(err)
    })
  })

  it('.exists(bucket,key,fn) -> (404)',function(done){
    db.exists('test-users','test+text@email.com',function(err,exists,meta){
      meta.should.have.status(404)
      exists.should.equal(false)
      done(err)
    })
  })

  it('.buckets(fn)',function(done){
    db.buckets(function(err,buckets){
      buckets.should.be.an.instanceof(Array)
      buckets.should.include('test-users')
      bucket = buckets[0]
      done(err)
    })
  })

  it('.getBucket(bucket,fn)',function(done){
    db.getBucket(bucket,function(err,props){
      props.should.have.property('name',bucket)
      props.should.have.property('r')
      props.should.have.property('allow_mult')
      done(err)
    })
  })

  it('.resources(fn)',function(done){
    db.resources(function(err,resources){
      resources.should.have.property('riak_kv_wm_buckets')
      resources.should.have.property('riak_kv_wm_props')
      done(err)
    })
  })

  it('.ping(fn)',function(done){
    db.ping(function(err,pong){
      pong.should.equal(true)
      done(err)
    })
  })

  it('.stats(fn)',function(done){
    db.stats(function(err,stats){
      stats.should.have.property('nodename','riak@127.0.0.1')
      done(err)
    })
  })

  it('should emit error when trying to connect to non-existent instance',function(done){
    var db2 = new Client({port:64208})
    db2.on('error',function(err){
      err.should.have.property('code','ECONNREFUSED')
      done()
    })
    db2.get('test-users','test+text@email.com')
  })

  describe('streaming keys',function(){
    var many = new Array(500).join().split(',').map(function(x,i){return i})

    // remove all previous keys in "test-keys"
    before(function(done){
      var pending = 0
        , keys = [];
      function gone(err){
        assert(!err,err)
        --pending || done()
      }
      db.keys('test-keys')
        .on('keys',function(keys){
          pending += keys.length
          keys.forEach(function(key){
            db.remove('test-keys',key,gone)
          })
        })
        .on('error',done)
        .on('end',function(){
          pending || done()
        })
        .start()
    })

    it('should create 500 keys',function(done){
      var pending = many.length;
      function next(err){
        assert(!err,err)
        --pending || done()
      }
      many.forEach(function(key){
        // forcing key to string because `0` will make it a POST
        db.save('test-keys',key.toString(),key,next);
      })
    })

    // FIXME this one fails...
    //       probably because of the ones that was POSTed
    // it('should stream those 500 keys',function(done){
    //   var keys = [];
    //   db.keys('test-keys')
    //     .on('keys',function(k){keys = keys.concat(k)})
    //     .on('error',done)
    //     .on('end',function(){
    //       // sorted and stringified for a nice diff error
    //       var m = many.sort().join('\n')
    //         , k = keys.sort().join('\n')
    //       m.should.eql(k)
    //       done()
    //     })
    //     .start()
    // })

    it('should count keys',function(done){
      db.count('test-keys',function(err,count){
        count.should.equal(many.length)
        done(err)
      })
    })

  })

  describe('map reduce',function(){

    // FIXME this one fails with status 500
    //       probably because of "test+text@email.com" (the + being url decoded away...)
    // it('should map users',function(done){
    //   db.add('test-users').map('Riak.mapValuesJson').run(function(err,users,meta){
    //     // TODO assertions...
    //     meta.should.have.status(200)
    //     done(err)
    //   })
    // })

  })


  // FIXME fails with "indexes_not_supported" error
  // need to configure riak_kv_eleveldb_backend
  describe('secondary indices',function(){
    return

    it('should save with index',function(done){
      db.save('test-users','fran@email.com',{age:28}, {index: {age:28, alias: 'fran'}},done)
    })

    it('should query by age',function(done){
      db.query('test-users',{ age: [20,30] },function(err,keys){
        keys.should.include('fran@email.com')
        done(err);
      })
    })

    it('should query by alias',function(done){
      db.query('test-users',{ alias: 'fran' },function(err,keys){
        keys.should.include('fran@email.com')
        done(err);
      })
    })

  })

  describe('custom meta',function(){

    function CustomMeta(){
      Meta.apply(this, arguments);
    }
    util.inherits(CustomMeta, Meta);

    var _parse = Meta.prototype.parse;
    CustomMeta.prototype.parse = function(data) {
      var result = _parse.call(this, data);
      if (result instanceof Object) result.intercepted = true;
      return result;
    }

    it('should fetch with a custom meta',function(done){
      var meta = new CustomMeta();
      db.get('test-users','fran@email.com', meta, function(err,user,meta){
        user.should.have.property('intercepted',true)
        done(err)
      })
    })

  })


})