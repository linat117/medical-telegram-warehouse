"""
Generate a visual star schema diagram from the data warehouse structure.

This script creates a professional diagram showing the relationships between
fact and dimension tables in the star schema.
"""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np


def create_star_schema_diagram(output_path='docs/star_schema_diagram.png'):
    """Generate a visual star schema diagram."""
    
    # Create figure with white background
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')
    
    # Define colors
    fact_color = '#FFD700'  # Gold for fact table
    dim_color = '#87CEEB'   # Sky blue for dimension tables
    text_color = '#000000'  # Black text
    
    # Center position for fact table
    fact_x, fact_y = 5, 5
    
    # Fact table dimensions
    fact_width = 3.5
    fact_height = 4.5
    
    # Draw fact table (fct_messages)
    fact_box = FancyBboxPatch(
        (fact_x - fact_width/2, fact_y - fact_height/2),
        fact_width, fact_height,
        boxstyle="round,pad=0.1",
        facecolor=fact_color,
        edgecolor='black',
        linewidth=2
    )
    ax.add_patch(fact_box)
    
    # Fact table title
    ax.text(fact_x, fact_y + 1.8, 'fct_messages', 
            ha='center', va='center', fontsize=14, fontweight='bold', color=text_color)
    ax.text(fact_x, fact_y + 1.5, '(Fact Table)', 
            ha='center', va='center', fontsize=10, style='italic', color=text_color)
    
    # Fact table attributes
    fact_attrs = [
        'message_id (PK)',
        'channel_key (FK)',
        'date_key (FK)',
        'message_text',
        'message_length',
        'view_count',
        'forward_count',
        'has_image'
    ]
    
    y_start = fact_y + 1.0
    for i, attr in enumerate(fact_attrs):
        ax.text(fact_x - fact_width/2 + 0.1, y_start - i*0.25, attr,
                ha='left', va='center', fontsize=9, color=text_color,
                family='monospace')
    
    # Dimension table positions
    dim_channels_x, dim_channels_y = 1.5, 8
    dim_dates_x, dim_dates_y = 8.5, 8
    
    # Draw dim_channels dimension table
    dim_width = 2.5
    dim_height = 3.5
    
    dim_channels_box = FancyBboxPatch(
        (dim_channels_x - dim_width/2, dim_channels_y - dim_height/2),
        dim_width, dim_height,
        boxstyle="round,pad=0.1",
        facecolor=dim_color,
        edgecolor='black',
        linewidth=2
    )
    ax.add_patch(dim_channels_box)
    
    ax.text(dim_channels_x, dim_channels_y + 1.4, 'dim_channels',
            ha='center', va='center', fontsize=12, fontweight='bold', color=text_color)
    ax.text(dim_channels_x, dim_channels_y + 1.1, '(Dimension Table)',
            ha='center', va='center', fontsize=9, style='italic', color=text_color)
    
    dim_channels_attrs = [
        'channel_key (PK)',
        'channel_name',
        'channel_type',
        'first_post_date',
        'last_post_date',
        'total_posts',
        'avg_views'
    ]
    
    y_start = dim_channels_y + 0.7
    for i, attr in enumerate(dim_channels_attrs):
        ax.text(dim_channels_x - dim_width/2 + 0.1, y_start - i*0.22, attr,
                ha='left', va='center', fontsize=8, color=text_color,
                family='monospace')
    
    # Draw dim_dates dimension table
    dim_dates_box = FancyBboxPatch(
        (dim_dates_x - dim_width/2, dim_dates_y - dim_height/2),
        dim_width, dim_height,
        boxstyle="round,pad=0.1",
        facecolor=dim_color,
        edgecolor='black',
        linewidth=2
    )
    ax.add_patch(dim_dates_box)
    
    ax.text(dim_dates_x, dim_dates_y + 1.4, 'dim_dates',
            ha='center', va='center', fontsize=12, fontweight='bold', color=text_color)
    ax.text(dim_dates_x, dim_dates_y + 1.1, '(Dimension Table)',
            ha='center', va='center', fontsize=9, style='italic', color=text_color)
    
    dim_dates_attrs = [
        'full_date (PK)',
        'day_of_month',
        'day_name',
        'week_of_year',
        'month',
        'month_name',
        'quarter',
        'year',
        'is_weekend'
    ]
    
    y_start = dim_dates_y + 0.7
    for i, attr in enumerate(dim_dates_attrs):
        ax.text(dim_dates_x - dim_width/2 + 0.1, y_start - i*0.22, attr,
                ha='left', va='center', fontsize=8, color=text_color,
                family='monospace')
    
    # Draw arrows from fact to dimensions
    # Arrow from fact to dim_channels (channel_key)
    arrow1 = FancyArrowPatch(
        (fact_x - fact_width/2, fact_y + 0.3),  # from channel_key
        (dim_channels_x + dim_width/2, dim_channels_y - 0.5),
        arrowstyle='->', mutation_scale=20, linewidth=1.5,
        color='black', connectionstyle='arc3,rad=0.2'
    )
    ax.add_patch(arrow1)
    ax.text(2.5, 6.5, 'channel_key', ha='center', va='center', 
            fontsize=8, style='italic', color='darkblue', fontweight='bold')
    
    # Arrow from fact to dim_dates (date_key)
    arrow2 = FancyArrowPatch(
        (fact_x + fact_width/2, fact_y + 0.05),  # from date_key
        (dim_dates_x - dim_width/2, dim_dates_y - 0.5),
        arrowstyle='->', mutation_scale=20, linewidth=1.5,
        color='black', connectionstyle='arc3,rad=-0.2'
    )
    ax.add_patch(arrow2)
    ax.text(7.5, 6.5, 'date_key', ha='center', va='center',
            fontsize=8, style='italic', color='darkblue', fontweight='bold')
    
    # Add title
    ax.text(5, 9.5, 'Star Schema: Medical Telegram Warehouse', 
            ha='center', va='center', fontsize=16, fontweight='bold', color=text_color)
    
    # Add legend
    fact_legend = mpatches.Patch(color=fact_color, label='Fact Table', edgecolor='black', linewidth=1)
    dim_legend = mpatches.Patch(color=dim_color, label='Dimension Table', edgecolor='black', linewidth=1)
    ax.legend(handles=[fact_legend, dim_legend], loc='lower center', 
              bbox_to_anchor=(0.5, 0.02), ncol=2, fontsize=10, frameon=True)
    
    # Save figure
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"Star schema diagram saved to: {output_path}")
    
    return fig


if __name__ == '__main__':
    import os
    
    # Create docs directory if it doesn't exist
    os.makedirs('docs', exist_ok=True)
    
    # Generate the diagram
    create_star_schema_diagram('docs/star_schema_diagram.png')
    print("Diagram generation complete!")
