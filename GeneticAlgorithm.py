import pygad
import numpy

inputs = [0,1,0,1,1,0,0,0,1,0,1,0,0,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
desired_output = 32


def fitness_func(solution, solution_idx):
    i = 1
    j = 4
    k = 1
    m  = 0
    cost = 180
    for sol in solution:
        j += i*sol
        i += j*sol - 0.2*i*j*sol
        k += -k*sol + 0.1*j*sol
        #m += 0.1*j*j - 0.5*i*k
        cost += sol*i + sol*j + sol*k #+ sol*m
        #print(cost)
    return cost


ga_instance = pygad.GA(num_generations=100,
                       num_parents_mating=3,
                       sol_per_pop=10,
                       num_genes=len(inputs),
                       fitness_func=fitness_func,
                       init_range_low=0,
                       init_range_high=2,
                       mutation_type='swap',

                       gene_type=int)

print(ga_instance.initial_population)

ga_instance.run()

ga_instance.plot_fitness()