var HttpClient = require('../lib/http-client'),
  HttpMeta = require('../lib/http-meta'),
  seq = require('seq'),
  util = require('util'),
  assert = require('assert'),
  test = require('../lib/utils').test;

describe('http client',function(){

  var db = new HttpClient()
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

  it('should emit error when trying to connect to non-existent instance',function(done){
    var db2 = new HttpClient({port:64208})
    db2.on('error',function(err){
      err.should.exist
      done()
    })
    db2.get('test-users','test+text@email.com')
  })

  describe('streaming keys',function(){
    var many = new Array(500).join().split(',').map(function(x,i){return i})

    before(function(done){
      // TODO remove all previous keys in "test-keys"
      done()
    })

    it('should create 500 keys',function(done){
      var pending = many.length;
      function next(err){
        assert(!err,err)
        --pending || done()
      }
      many.forEach(function(key){
        db.save('test-keys',key.toString(),key,next);
      })
    })

    // FIXME this one fails...
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
    // probably because of "test+text@email.com" (the + being url decoded away...)
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



})

return

seq()
  
  .seq(function(buckets) {
    test('Get the properties of a bucket');
    var bucket = buckets[0];
    db.getBucket(bucket, this);
  })
  .seq(function(props) {
    assert.ok(props && props.r);
    this.ok()
  })
  
  .seq(function() {
    test("List resources");
    db.resources(this);
  })
  .seq(function(resources) {
    assert.ok(resources && resources.riak_kv_wm_buckets);
    this.ok();
  })
  
  .seq(function() {
    test('Ping');
    db.ping(this);
  })
  .seq(function(pong) {
    assert.ok(pong);
    this.ok()
  })
  
  .seq(function() {
    test('Stats');
    db.stats(this);
  })
  .seq(function(stats) {
    assert.ok(stats.riak_core_version);
    this.ok();
  })
  
  .seq(function() {
    test('Custom Meta');
    var meta = new CustomMeta();
    db.get('users', 'test2@gmail.com', meta, this);
  })
  .seq(function(user) {
    assert.equal(user.intercepted, true);
    this.ok();
  })
    
  .catch(function(err) {
    console.log(err.stack);
    process.exit(1);
  });
  
/* Custom Meta */

var CustomMeta = function() {
  var args = Array.prototype.slice.call(arguments);
  HttpMeta.apply(this, args);
}

util.inherits(CustomMeta, HttpMeta);

CustomMeta.prototype.parse = function(data) {
  var result = HttpMeta.prototype.parse.call(this, data);
  if (result instanceof Object) result.intercepted = true;
  return result;
}