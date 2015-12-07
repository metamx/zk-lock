module.exports = (grunt) ->
  'use strict'
  grunt.loadNpmTasks('grunt-contrib-copy')
  grunt.loadNpmTasks('grunt-contrib-watch')
  grunt.loadNpmTasks('grunt-ts')
  grunt.loadNpmTasks('grunt-tslint')
  grunt.loadNpmTasks('grunt-tsd')
  grunt.loadNpmTasks('dts-generator')

  config =
    ts:
      options:
        target: 'es5'
        module: 'commonjs'
        declaration: true
        sourceMap: true
        emitDecoratorMetadata: true
        failOnTypeErrors: false
      build:
        src:  [ 'src/*.ts', 'typings/**/*.ts', 'typings_custom/*.ts' ]
        outDir: 'build/'
        baseDir: 'src/'
        options:
          sourceMap: true
          noEmitHelpers: true

    tslint:
      options:
        configuration: grunt.file.readJSON("tslint.json")
      files:
        src: [ 'src/**/*.ts' ]

    tsd:
      load:
        options:
          command: 'reinstall'
          latest: false
          config: 'tsd.json'
      refresh:
        options:
          command: 'reinstall'
          latest: true
          config: 'tsd.json'

    dtsGenerator:
      options:
        name: 'zk-lock'
        baseDir: 'src/'
        out: 'build/zk-lock.d.ts'
      default:
        src: [ 'src/**/*.ts' ]



    watch:
      typescripts:
        files: 'src/**/*.ts'
        tasks: [ 'watch-compile' ]
        options:
          livereload: true


  grunt.initConfig(config)

  grunt.registerTask 'default', [
    'tsd:load',
    'tslint',
    'ts:build',
    'dtsGenerator'
  ]

  grunt.registerTask 'watch-compile', [
    'tslint',
    'ts:build',
    'dtsGenerator'
  ]

  return