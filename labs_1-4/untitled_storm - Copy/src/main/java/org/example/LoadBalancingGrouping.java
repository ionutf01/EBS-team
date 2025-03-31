package org.example;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadBalancingGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;
    private Map<Integer, Double> taskLoads;
    private Map<Integer, List<Double>> taskLoadHistory;
    private final int windowSize = 10; // Mărimea ferestrei pentru calculul mediei încărcării

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        this.taskLoads = new HashMap<>();
        this.taskLoadHistory = new HashMap<>();

        // Inițializăm încărcarea pentru fiecare task cu 0
        for (Integer taskId : targetTasks) {
            taskLoads.put(taskId, 0.0);
            taskLoadHistory.put(taskId, new ArrayList<>());
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> result = new ArrayList<>(1);

        // Extragem dificultatea de procesare din tuplu (ultimul element)
        Double processingDifficulty = (Double) values.get(values.size() - 1);

        // Identificăm task-ul cu încărcarea minimă
        Integer minLoadTaskId = findMinLoadTask();

        // Actualizăm încărcarea pentru task-ul ales
        updateTaskLoad(minLoadTaskId, processingDifficulty);

        result.add(minLoadTaskId);

        // Afișăm încărcarea curentă a task-urilor pentru debugging
        System.out.println("Task loads: " + taskLoads);

        return result;
    }

    private Integer findMinLoadTask() {
        Double minLoad = Double.MAX_VALUE;
        Integer minLoadTaskId = targetTasks.get(0);

        for (Integer taskId : targetTasks) {
            Double currentLoad = taskLoads.get(taskId);
            if (currentLoad < minLoad) {
                minLoad = currentLoad;
                minLoadTaskId = taskId;
            }
        }

        return minLoadTaskId;
    }

    private void updateTaskLoad(Integer taskId, Double additionalLoad) {
        // Adăugăm noua încărcare la istoricul task-ului
        List<Double> history = taskLoadHistory.get(taskId);
        history.add(additionalLoad);

        // Menținem doar ultimele 'windowSize' valori în istoric
        if (history.size() > windowSize) {
            history.remove(0);
        }

        // Recalculăm încărcarea medie pentru task
        Double totalLoad = 0.0;
        for (Double load : history) {
            totalLoad += load;
        }

        Double averageLoad = totalLoad / history.size();
        taskLoads.put(taskId, averageLoad);
    }
}