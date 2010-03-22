# -*- encoding: utf-8 -*-
begin
  require 'jeweler'

  Jeweler::Tasks.new do |s|
    s.name     = "blaxter-delayed_job"
    s.summary  = "Database-backed asynchronous priority queue system -- Extracted from Shopify"
    s.email    = "blaxter@gmail.com"
    s.homepage = "http://github.com/blaxter/delayed_job"
    s.description = "Delayed_job (or DJ) encapsulates the common pattern of asynchronously executing longer tasks in the background. It is a direct extraction from Shopify where the job table is responsible for a multitude of core tasks."
    s.authors  = ["Tobias Lütke", "Jesús García Sáez"]

    s.has_rdoc = true
    s.rdoc_options = ["--main", "README.textile", "--inline-source", "--line-numbers"]
    s.extra_rdoc_files = ["README.textile"]

    s.test_files = Dir['spec/**/*']
  end

rescue LoadError
  puts "Jeweler not available. Install it with: sudo gem install technicalpickles-jeweler -s http://gems.github.com"
  exit 1
end

require 'spec/rake/spectask'
Spec::Rake::SpecTask.new(:spec) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.spec_files = FileList['spec/**/*_spec.rb']
end

Spec::Rake::SpecTask.new(:rcov) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
end


task :default => :spec
