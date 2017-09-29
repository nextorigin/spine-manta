Spine        = require "spine"
Manta        = require "manta"
errify       = require "errify"


# capitalize = (str) -> str[0].toUpperCase() + str[1..]


class SpineManta extends Spine.Model
  @setup: (server) ->
    {signer, user, subuser, url, @namespace} = server

    sign   = Manta.privateKeySigner signer
    @manta = Manta.createClient {sign, user, subuser, url}
    @_manta_url = url
    @attributes.push "type"

  @url: ->
    "/#{@manta.user}/#{@namespace}/#{@className}"

  @fullPath: (id) ->
    "#{@url()}/#{id}"

  @idFromPath: (path) ->
    start = path.indexOf "#{@url()}/"
    end   = start + path.length
    path[end..]

  @bootstrap: (callback) ->
    @manta.mkdirp @url(), callback

  @parseRow: (stream, res, id) ->
    attrs      = {}
    attrs[key] = res.headers[key] for key in @attributes when "m-#{key}" of res.headers
    attrs.id   = id
    attrs.contentType = res.headers["content-type"]

    record     = new @ attrs
    record.exists = true
    record.stream = stream
    record

  @makeRecords: (rows) ->
    records = (@parseRow row for row in rows when row)
    records

  @find: (id, cb) ->
    ideally = errify cb

    await @manta.get (@fullPath id), ideally defer stream, res
    cb null, @parseRow stream, res, id

  # @findAll: (options = {}, cb = ->) ->
  #   (cb = options) and options = {} if typeof options is "function"
  #   ideally = errify cb

  #   await (@manta.findAll options).asCallback ideally defer rows
  #   cb null, @makeRecords rows

  # @findMany: (ids, cb = ->) ->
  #   ideally = errify cb
  #   query =
  #     where: id: ids

  #   await (@manta.findAll query).asCallback ideally defer rows
  #   cb null, @makeRecords rows

  # @findAllById: (key, value, options, cb = ->) ->
  #   return @findMany value, cb if Array.isArray value
  #   @find value, cb

  type: -> @constructor.className

  attributes: (hideId) ->
    result = super()
    delete result.id if hideId
    result

  exists: -> Boolean @_exists

  save: (cb = ->) ->
    ideally = errify cb
    wasNew  = @isNew()

    if wasNew
      @[key] = value for key, value of @constructor.defaults when not @[key]?
      await @constructor.manta.put @_fullPath(), @stream, @_mantaOptions(), ideally defer()
      @_exists = true
    else
      await @constructor.manta.put @_fullPath(), @stream, @_mantaOptions(), ideally defer()

    cb null, this

  remove: (cb = ->) ->
    @constructor.manta.unlink @_fullPath(), cb

  _fullPath: ->
    @constructor.fullPath @id

  _mantaOptions: ->
    type    = @contentType or "application/octet-stream"
    headers = {}
    headers["m-#{key}"] = value for key, value of (@attributes true) when key isnt "contentType"

    {type, headers}


module.exports = SpineManta
