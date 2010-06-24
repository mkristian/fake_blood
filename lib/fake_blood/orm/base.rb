require "benchmark"
require "stringio"
require "pp"

module Base
  module Constants
    BIO_ = <<-END
    Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
    Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
    Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum
    END
    BIO = BIO_ * 100

  end

  class DBTest
    include Benchmark  

    def initialize( opts = {} )
      @times = opts[:times] || 1
      @transaction = opts[:transaction] || false
      @name = opts[:name] || self.class.to_s
    end

    def test_name()
      @name
    end

    def up()
    end

    def down()
    end

    # Test in fact
    def run()
      raise NotImplementedError
    end

    def run_opt()
      run()
    end

    def run!()
      run_with_benchmark("#{@name};#{@transaction ? 1:0};0"){ run( ) }
    end

    def run_opt!()
      run_with_benchmark("#{@name};#{@transaction ? 1:0};1"){ run_opt( ) }
    end

    private

      def run_with_benchmark( bench_name, &block )
        report_std = ""
        reports = []
        total, avg = nil, nil
        old_stdout, $stdout = $stdout, new_stdout = StringIO.new

        Benchmark.benchmark(" "*7 + CAPTION, 7, FMTSTR, ">total:", ">avg:") do |benchmark|
          @times.times do |test_num|
            up()
             @name = "#{bench_name}" 
            report = benchmark.report(@name, &block)
            reports << report
            report.format('%u;%t;%r').gsub(/[()]/, '')
            down()
          end
          total = summed_reports = reports.inject { |i, j| i+j }
          avg = summed_reports / @times
          [total, avg]
        end
         $stdout = old_stdout
         report_std = new_stdout.string()
        # puts report_std
        {
                :total => total.format('%u;%t;%r').gsub(/[()]/, ''),
                :avg => avg.format('%u;%t;%r').gsub(/[()]/, ''),
                :times => @times,
                :name => @name,
                :transaction => @transaction,
                :report => report_std
        }
      end
  end
end

