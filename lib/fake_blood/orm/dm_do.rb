require File.join(File.dirname(__FILE__), 'base')
require "rubygems"   # rm that  
require 'dm-core'
require 'dm-migrations'
require 'dm-transactions'
#require 'dm-constraints'

module DataMapperOrm

  class Person
     include DataMapper::Resource

     property    :id,      Serial
     property    :name,    String
     property    :address, String
     property    :bio,     Text

     belongs_to  :party
     # XXX indexes  by default!
   end

   class Party
     include DataMapper::Resource

     property    :id,     Serial
     property    :theme,  String

     has n,      :people
   end
  

  def self.setup( opts = {})
    connection_options = opts[:connection_options] ||
            {         
               :orm => "data_mapper",
               :adapter => "hibernate", 
               :dialect => "H2", 
               :username => "sa", 
               :url => "jdbc:h2:target/simple_orm_benchmark_h2"
            }
    # DataMapper::Logger.new($stdout, :debug)
    DataMapper.setup(:default, connection_options)

    self.up()
  end

  def self.up
    DataMapper.finalize()
    DataMapper.auto_migrate!()
  end

  def self.finalize
    Party.auto_migrate_down!()
    Person.auto_migrate_down!()
  end

  module Tests
    class DmTest < Base::DBTest

      def initialize( opts = {})
        super( opts )
        @how_many_parties = opts[:parties] || 1
        @how_many_people = opts[:people] || 1
      end

      def up()
        DataMapper.auto_migrate!()
        
        @how_many_parties.times do |num|
          party = Party.new({ :theme => "Halloween" })
          @how_many_people.times do
            party.people.new({ :name => "Party_#{num}" })
          end
          party.save()
        end
      end

      def down()
        Person.destroy!()
        Party.destroy!()
        Party.auto_migrate_down!()
        Person.auto_migrate_down!()
      end

    end

    # ========== TEST CASES ===============

    class GetFirstParty < DmTest
      def run()
        Party.first().theme()
      end
    end

    class GetFirstPersonFromFirstParty < DmTest
      def run()
        Party.first().people().first().name()
      end

      def run_opt()
        Party.first( :fields => [ :id ]).people( :fields => [:name]).first().name()        
      end
    end

    class GetFirstPersonFromFirstPartyTwice < DmTest
      def run()
        Party.first().people().first().name()
        Party.first().people().first().name()
      end

      def run_opt()
        Party.first( :fields => [ :id ]).people(:fields => [:name]).first().name()
        Party.first( :fields => [ :id ]).people(:fields => [:name]).first().name()
      end
    end

    class GetPersonById < DmTest
      def run()
        Person.get(1).name
      end
    end

    class GetPersonByIdTwice < DmTest
      def run()
        Person.get(1).name
        Person.get(1).name
      end
    end

    class GetPersonByIdWithHeavyField < DmTest
      def run()
        Person.get(1).bio
      end

      def run_opt()
        Person.first( :id => 1, :fields => [ :id, :bio ]).bio        
      end
    end

    class GetPersonByIdWithHeavyFieldTwice < DmTest
      def run()
        Person.get(1).bio
        Person.get(1).bio
      end

      def run_opt()
        Person.first( :id => 1, :fields => [ :id, :bio ]).bio
        Person.first( :id => 1, :fields => [ :id, :bio ]).bio
      end
    end
    
    class InsertParty < DmTest
      def up()
        DataMapperOrm.up()
      end

      def run()
        party = Party.new({ :theme => "Halloween" })
        party.save()
      end

      def run_opt()
        party = Party.create({ :theme => "Halloween" })
      end
    end
    
    class InsertPartyAndPeople < DmTest
      def up()
        DataMapperOrm.up()
      end

      def run()
        @how_many_parties.times do |num|
          party = Party.new({ :theme => "Halloween" })
          @how_many_people.times do
            party.people.new({ :name => "Party_#{num}" })
          end
          party.save()
        end
      end
    end

    class InsertPartyAndPeopleWithHeavyField < DmTest
      def up()
        DataMapperOrm.up()
      end

      def run()
        @how_many_parties.times do |num|
          party = Party.new({ :theme => "Halloween" })
          @how_many_people.times do
            party.people.new({ :name => "Party_#{num}",  :bio => Base::Constants::BIO})
          end
          party.save()
        end
      end
    end

    class LoadPeople < DmTest
      def run()
        Person.all( ).each do |person|
          person.id
        end
      end
    end

    class LoadPeopleWithHeavyField < DmTest
      def run()
        Person.all( ).each do |person|
          person.id
          person.bio
        end
      end

      def run_opt()
        Person.all( :fields => [ :id, :bio ]).each do |person|
          person.id
          person.bio
        end
      end
    end

    class LoadPartyAndPeople < DmTest
      def run
        Party.all().each do |party|
          party.people( ).each do |person|
            person.id
          end
        end
      end

      def run_opt
        Party.all( :fields => [ :id ] ).each do |party|
          party.people( :fields => [ :id ] ).each do |person|
            person.id
          end
        end
      end
    end

    class LoadPartyAndPeopleWithHeavyField < DmTest
      def run
        Party.all( ).each do |party|
          party.people( ).each do |person|
            person.id
            person.bio
          end
        end
      end

      def run_opt
        Party.all( :fields => [ :id ] ).each do |party|
          party.people( :fields => [ :id, :bio ] ).each do |person|
            person.id
            person.bio
          end
        end
      end
    end

    class LoadPartyAndPeopleTwice < DmTest
      def run
        Party.all( ).each do |party|
          party.people( ).each do |person|
            person.id
          end
          party.people(  ).each do |person|
            person.id
          end          
        end
      end

      def run_opt
        Party.all( :fields => [ :id ] ).each do |party|
          party.people( :fields => [ :id ] ).each do |person|
            person.id
          end
          party.people( :fields => [ :id ] ).each do |person|
            person.id
          end
        end
      end
    end

    class LoadPartyAndPeopleWithHeavyFieldTwice < DmTest
      def run
        Party.all( ).each do |party|
          party.people( ).each do |person|
            person.id
            person.bio
          end
          party.people( ).each do |person|
            person.id
            person.bio
          end
        end
      end

      def run_opt
        Party.all( :fields => [ :id ] ).each do |party|
          party.people( :fields => [ :id, :bio ] ).each do |person|
            person.id
            person.bio
          end
          party.people( :fields => [ :id, :bio ] ).each do |person|
            person.id
            person.bio
          end
        end
      end
    end

    class GetPersonByNameAndUpdateName < DmTest
      def run()
        person = Person.first( :name => "Party_0" )
        person.name = "Party_1_modified"
        person.save()
      end

      def run_opt()
        person = Person.first( :name => "Party_0 ", :fields => [ :id, :name ])
        person.name = "Party_1_modified"
        person.save()
      end
    end

    class GetPersonByNameAndUpdateBio < DmTest
      def run()
        person = Person.first( :name => "Party_0" )
        person.bio = "cart blanche"
        person.save()
      end

      def run_opt()
        person = Person.first( :name => "Party_0", :fields => [ :id, :name, :bio ])
        person.bio = "cart blanche"
        person.save()
      end
    end

    class DestroyFirstPerson < DmTest
      def run()
        Person.first().destroy
      end

      def run_opt()
        Person.first( :fields => [:id] ).destroy        
      end
    end

#
#    def eager_graph_load_party_and_people_with_bio
#      #    Party.find(:all, :include=>[:people, :other_people], :conditions=>'people.id=people.id AND
#      #    other_peoples_parties.id=other_peoples_parties.id').each{|party| party.people.each{|p| p.id};
#      #    party.other_people.each{|p| p.id}}
#      # Party.all( :conditions => [ " people.id = people.id" ] ).each do |party|
#
#      # Party.all( :links => [ Party.relationships[:people].inverse ], :people.not => nil ).each do |party|
#      Party.all( :fields => [ :id ], :people.not => nil ).each do |party|
#        party.people( :fields => [ :id, :bio ] ).each do |person|
#          person.id
#          person.bio
#        end
#      end
#    end
#
#    def eager_graph_load_party_and_people
#      #    Party.find(:all, :include=>:people, :conditions=>'people.id=people.id').each{|party| party.people.each{|p| p.id}}
#      # XXX Bug !
#      #Party.all( :links => [ :people ], :people.not => nil ).each do |party|
#      # Party.all( :links => [ Party.relationships[:people].inverse ], :people.not => nil ).each do |party|
#      Party.all(  :fields => [ :id ], :people.not => nil ).each do |party|
#        party.people( :fields => [ :id ] ).each do |person|
#          person.id
#        end
#      end
#    end
    

  end
end

DataMapperOrm.setup()
TESTS =
[
  #  DataMapperOrm::Tests::GetFirstParty,
  #  DataMapperOrm::Tests::GetFirstPersonFromFirstParty,
  #  DataMapperOrm::Tests::GetFirstPersonFromFirstPartyTwice,
  #  DataMapperOrm::Tests::GetPersonById,
  #  DataMapperOrm::Tests::GetPersonByIdTwice,
  #  DataMapperOrm::Tests::GetPersonByIdWithHeavyField,
  #  DataMapperOrm::Tests::GetPersonByIdWithHeavyFieldTwice,
  #  DataMapperOrm::Tests::InsertPartyAndPeople,
  #  DataMapperOrm::Tests::InsertPartyAndPeopleWithHeavyField,
  #  DataMapperOrm::Tests::GetPersonByNameAndUpdateName,
  #  DataMapperOrm::Tests::GetPersonByNameAndUpdateBio,
  #  DataMapperOrm::Tests::DestroyFirstPerson,
  DataMapperOrm::Tests::LoadPeople,
  DataMapperOrm::Tests::LoadPeopleWithHeavyField,
  DataMapperOrm::Tests::LoadPartyAndPeople,
  DataMapperOrm::Tests::LoadPartyAndPeopleTwice,
  DataMapperOrm::Tests::LoadPartyAndPeopleWithHeavyField,
  DataMapperOrm::Tests::LoadPartyAndPeopleWithHeavyFieldTwice,

]
reports = []

# TODO pomiar dla 1 bloku?

# XXX pierw/ost pomiar zawyzony??? ( pob. polaczenia )

# def transaction(&block)
#    DataMapper::Transaction.new(DataMapper.repository(:default)).commit(&block)
#  end
#
#  def with_connection
#    yield
#  end

# TODO: watki!
# TODO: cpu, pamiec !

# TODO DataObjects -dane

# TODO: struktura CSV
# TODO: results agregator!
# TODO: server mode/client mode
# TODO: statystyka, odchyl itd (excel) ?




TESTS.each do |test_klazz|
  puts "#{test_klazz}====================================="
  # [1, 10, 100].each do |parties|
  [1].each do |parties|
    # [1, 10, 100, 1000, 10000].each do |people|
    [10].each do |people|
      #[20, 10, 1].each do |times|
      [1, 10].each do |times|
        [true, false].each do |transaction|
          report = test_klazz.new( :people => people, :parties => parties, :times => times, :transaction => transaction ).run!()
          puts "#{report[:name]};#{report[:total]};#{report[:avg]};#{report[:times]};#{parties};#{people}"
          
          report = test_klazz.new( :people => people, :parties => parties, :times => times, :transaction => transaction ).run_opt!()
          puts "#{report[:name]};#{report[:total]};#{report[:avg]};#{report[:times]};#{parties};#{people}"
        end
      end
    end
  end
end

DataMapperOrm.finalize()
